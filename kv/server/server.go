package server

import (
	"context"
	"encoding/hex"
	"fmt"
	"reflect"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/log"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
	"go.uber.org/zap"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.GetResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionerr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionerr.RequestErr
			return resp, err
		}
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.Version)
	// 检查锁
	lock, err := txn.GetLock(req.Key)
	if err != nil {
		// 出错了
		return nil, err
	}
	// 有lock
	if lock != nil && lock.IsLockedFor(req.Key, txn.StartTS, resp) {
		return resp, nil
	}

	value, err := txn.GetValue(req.Key)
	if err != nil {
		return nil, err
	}
	if value != nil {
		resp.Value = value
	} else {
		resp.NotFound = true
	}
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.PrewriteResponse{}
	result := [][]byte{}
	for _, m := range req.Mutations {
		result = append(result, m.Key)
	}
	server.Latches.WaitForLatches(result)
	defer server.Latches.ReleaseLatches(result)

	rder, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer rder.Close()

	// 构建txn
	txn := mvcc.NewMvccTxn(rder, req.StartVersion)
	for _, m := range req.Mutations {
		key := m.Key
		keyerror := new(kvrpcpb.KeyError)
		_, committs, err := txn.MostRecentWrite(key)
		if err != nil {
			return nil, err
		}
		if committs >= txn.StartTS {
			// 由于没有key返回最小committs 0,则不用判断是否有key对应
			keyerror.Conflict = &kvrpcpb.WriteConflict{StartTs: txn.StartTS, ConflictTs: committs, Key: key, Primary: req.PrimaryLock}
			resp.Errors = append(resp.Errors, keyerror)
			continue
		}

		// 检查写冲突
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock != nil && lock.Ts != txn.StartTS {
			keyerror.Locked = lock.Info(key)
			resp.Errors = append(resp.Errors, keyerror)
			continue
		}

		// 写lock，如果 当前primary情况？
		lock_txn := &mvcc.Lock{Primary: req.PrimaryLock, Ts: req.GetStartVersion(), Ttl: req.GetLockTtl(), Kind: mvcc.WriteKindFromProto(m.Op)}
		txn.PutLock(key, lock_txn)
		// 写data
		txn.PutValue(key, m.Value)
	}
	server.Latches.Validate(txn, result)

	// 完成txn
	err = server.storage.Write(req.GetContext(), txn.Writes())
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.CommitResponse{}
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	committs := req.CommitVersion
	if committs <= txn.StartTS {
		resp.Error = &kvrpcpb.KeyError{Retryable: fmt.Sprintf("commitTs %v <= txn.StartTs %v", committs, txn.StartTS)}
	}

	for _, k := range req.Keys {
		res, e := commitKey(k, committs, txn, resp)
		if res != nil || e != nil {
			return resp, e
		}
	}

	server.Latches.Validation(txn, req.Keys)

	// Building the transaction succeeded without conflict, write all writes to backing storage.
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func commitKey(key []byte, commitTs uint64, txn *mvcc.MvccTxn, response interface{}) (interface{}, error) {
	lock, err := txn.GetLock(key)
	if err != nil {
		return nil, err
	}

	// If there is no correspond lock for this transaction.
	log.Debug("commitKey", zap.Uint64("startTS", txn.StartTS),
		zap.Uint64("commitTs", commitTs),
		zap.String("key", hex.EncodeToString(key)))
	if lock == nil || lock.Ts != txn.StartTS {
		// YOUR CODE HERE (lab1).
		// Key is locked by a different transaction, or there is no lock on the key. It's needed to
		// check the commit/rollback record for this key, if nothing is found report lock not found
		// error. Also the commit request could be stale that it's already committed or rolled back.

		write, commit_ts, err := txn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}

		if write != nil && write.Kind != mvcc.WriteKindRollback || commit_ts > commitTs {
			return nil, nil
		}
		respValue := reflect.ValueOf(response)
		keyError := &kvrpcpb.KeyError{Retryable: fmt.Sprintf("lock not found for key %v", key)}
		reflect.Indirect(respValue).FieldByName("Error").Set(reflect.ValueOf(keyError))
		return response, nil
	}

	// Commit a Write object to the DB
	write := mvcc.Write{StartTS: txn.StartTS, Kind: lock.Kind}
	txn.PutWrite(key, commitTs, &write)
	// Unlock the key
	txn.DeleteLock(key)

	return nil, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ScanResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}

	txn := mvcc.NewMvccTxn(reader, req.Version)
	scanner := mvcc.NewScanner(req.StartKey, txn)
	defer scanner.Close()

	limit := req.Limit

	for {
		if limit == 0 {
			//
			return resp, nil
		}
		limit -= 1

		key, value, err := scanner.Next()
		if err != nil {
			// 如果是keyerror
			if e, ok := err.(*mvcc.KeyError); ok {
				pair := new(kvrpcpb.KvPair)
				pair.Error = &e.KeyError
				resp.Pairs = append(resp.Pairs, pair)
				continue
			} else {
				return nil, err
			}
		}
		if key == nil {
			return resp, nil
		}

		pair := kvrpcpb.KvPair{}
		pair.Key = key
		pair.Value = value
		resp.Pairs = append(resp.Pairs, &pair)
	}
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.CheckTxnStatusResponse{}
	primarykey := req.GetPrimaryKey()

	server.Latches.WaitForLatches([][]byte{primarykey})
	defer server.Latches.ReleaseLatches([][]byte{primarykey})

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.LockTs)

	lock, err := txn.GetLock(primarykey)
	if err != nil {
		return nil, err
	}
	if lock != nil && lock.Ts == txn.StartTS {
		// 有锁，且是本事务的锁
		// 检查锁的状态
		if mvcc.PhysicalTime(lock.Ts)+lock.Ttl < mvcc.PhysicalTime(req.CurrentTs) {
			// 锁应该扔掉
			resp.Action = kvrpcpb.Action_TTLExpireRollback
			resp.LockTtl = 0
			resp.CommitVersion = 0
			write := mvcc.Write{StartTS: txn.StartTS, Kind: mvcc.WriteKindRollback}
			txn.PutWrite(primarykey, txn.StartTS, &write)
			txn.DeleteLock(primarykey)
			txn.DeleteValue(primarykey)
			server.Latches.Validation(txn, [][]byte{primarykey})
			err = server.storage.Write(req.Context, txn.Writes())
			if err != nil {
				return nil, err
			}
		} else {
			resp.Action = kvrpcpb.Action_NoAction
			resp.LockTtl = lock.Ttl
		}
		return resp, nil
	}

	// 无锁
	existingwrite, committs, err := txn.CurrentWrite(primarykey)
	if err != nil {
		return nil, err
	}
	// 没有锁没有撤销
	if existingwrite == nil {
		resp.Action = kvrpcpb.Action_LockNotExistRollback
		resp.LockTtl = 0
		resp.CommitVersion = 0
		write := mvcc.Write{StartTS: txn.StartTS, Kind: mvcc.WriteKindRollback}
		txn.PutWrite(primarykey, txn.StartTS, &write)
		server.Latches.Validation(txn, [][]byte{primarykey})
		err = server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			return nil, err
		}
		return resp, nil
	}
	// 已经rollback了
	if existingwrite.Kind == mvcc.WriteKindRollback {
		resp.Action = kvrpcpb.Action_NoAction
		return resp, nil
	}

	// 已经提交
	resp.CommitVersion = committs
	resp.Action = kvrpcpb.Action_NoAction

	server.Latches.Validation(txn, [][]byte{primarykey})
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.BatchRollbackResponse{}
	keys := req.Keys

	// 上锁
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, k := range keys {
		res, err := rollbackKey(k, txn, resp)
		if res != nil || err != nil {
			return resp, err
		}
	}
	server.Latches.Validate(txn, keys)

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func rollbackKey(key []byte, txn *mvcc.MvccTxn, response interface{}) (interface{}, error) {
	lock, err := txn.GetLock(key)
	if err != nil {
		return nil, err
	}
	log.Info("rollbackKey",
		zap.Uint64("startTS", txn.StartTS),
		zap.String("key", hex.EncodeToString(key)))

	if lock == nil || lock.Ts != txn.StartTS {
		// There is no lock, check the write status.
		existingWrite, ts, err := txn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}
		// Try to insert a rollback record if there's no correspond records, use `mvcc.WriteKindRollback` to represent
		// the type. Also the command could be stale that the record is already rolled back or committed.
		// If there is no write either, presumably the prewrite was lost. We insert a rollback write anyway.
		// if the key has already been rolled back, so nothing to do.
		// If the key has already been committed. This should not happen since the client should never send both
		// commit and rollback requests.
		// There is no write either, presumably the prewrite was lost. We insert a rollback write anyway.
		if existingWrite == nil {
			// YOUR CODE HERE (lab1).
			// 没有这个txn相应的rollback
			write := mvcc.Write{StartTS: txn.StartTS, Kind: mvcc.WriteKindRollback}
			txn.PutWrite(key, txn.StartTS, &write)
			return nil, nil
		} else {
			if existingWrite.Kind == mvcc.WriteKindRollback {
				// The key has already been rolled back, so nothing to do.
				return nil, nil
			}

			// The key has already been committed. This should not happen since the client should never send both
			// commit and rollback requests.
			// 已经提交了，所以这个rollback是错误请求
			err := new(kvrpcpb.KeyError)
			err.Abort = fmt.Sprintf("key has already been committed: %v at %d", key, ts)
			respValue := reflect.ValueOf(response)
			reflect.Indirect(respValue).FieldByName("Error").Set(reflect.ValueOf(err))
			return response, nil
		}
	}

	if lock.Kind == mvcc.WriteKindPut {
		txn.DeleteValue(key)
	}

	write := mvcc.Write{StartTS: txn.StartTS, Kind: mvcc.WriteKindRollback}
	txn.PutWrite(key, txn.StartTS, &write)
	txn.DeleteLock(key)

	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ResolveLockResponse{}

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	txn.StartTS = req.StartVersion
	keylocks, err := mvcc.AllLocksForTxn(txn)
	if err != nil {
		return nil, err
	}
	keys := [][]byte{}
	for _, k := range keylocks {
		keys = append(keys, k.Key)
	}

	// 拿锁
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	for _, k := range keylocks {
		if req.CommitVersion == 0 {
			rollbackKey(k.Key, txn, resp)
		} else {
			commitKey(k.Key, req.CommitVersion, txn, resp)
		}
		if resp.Error != nil {
			server.Latches.Validate(txn, keys)

			err = server.storage.Write(req.Context, txn.Writes())
			if err != nil {
				return nil, err
			}
			return resp, nil
		}
	}
	server.Latches.Validate(txn, keys)

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
