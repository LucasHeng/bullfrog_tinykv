package server

import (
	"context"
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
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
		if regionError, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionError.RequestErr
			return resp, err
		}
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.Version)
	lock, err := txn.GetLock(req.Key)
	if err != nil {
		if regionError, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionError.RequestErr
			return resp, err
		}
		return nil, err
	}
	// 读写冲突
	if lock != nil && lock.Ts < req.Version {
		resp.Error = &kvrpcpb.KeyError{
			Locked: &kvrpcpb.LockInfo{
				PrimaryLock: lock.Primary,
				LockVersion: lock.Ts,
				Key:         req.Key,
				LockTtl:     lock.Ttl,
			},
		}
		return resp, nil
	}
	value, err := txn.GetValue(req.Key)
	if err != nil {
		if regionError, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionError.RequestErr
			return resp, err
		}
		return nil, err
	}
	resp.Value = value
	if value == nil {
		resp.NotFound = true
	}
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.PrewriteResponse{}
	// 对于空mutatition写请求
	if len(req.Mutations) == 0 {
		return resp, nil
	}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionError, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionError.RequestErr
			return resp, err
		}
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, m := range req.Mutations {
		write, commitTs, err := txn.MostRecentWrite(m.Key)
		if err != nil {
			if regionError, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionError.RequestErr
				return resp, err
			}
			return nil, err
		}
		// 写写冲突
		if write != nil && commitTs >= req.StartVersion {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{Conflict: &kvrpcpb.WriteConflict{
				StartTs:    req.StartVersion,
				ConflictTs: commitTs,
				Key:        m.Key,
				Primary:    req.PrimaryLock,
			}})
			continue
		}
		// 获取锁
		lock, err := txn.GetLock(m.Key)
		if err != nil {
			if regionError, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionError.RequestErr
				return resp, err
			}
			return nil, err
		}
		// 写写冲突
		if lock != nil && lock.Ts != req.StartVersion {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{Locked: &kvrpcpb.LockInfo{
				PrimaryLock: lock.Primary,
				LockTtl:     lock.Ttl,
				LockVersion: lock.Ts,
				Key:         m.Key,
			}})
			continue
		}
		preLock := &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
		}
		switch m.Op {
		case kvrpcpb.Op_Put:
			preLock.Kind = mvcc.WriteKindPut
			txn.PutValue(m.Key, m.Value)
		case kvrpcpb.Op_Del:
			preLock.Kind = mvcc.WriteKindDelete
			txn.DeleteValue(m.Key)
		}
		txn.PutLock(m.Key, preLock)
	}
	if len(resp.Errors) > 0 {
		return resp, nil
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionError, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionError.RequestErr
			return resp, err
		}
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.CommitResponse{}
	if len(req.Keys) == 0 {
		return resp, nil
	}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionError, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionError.RequestErr
			return resp, err
		}
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	if req.CommitVersion < txn.StartTS {
		resp.Error = &kvrpcpb.KeyError{Abort: "unexpect error"}
		return resp, nil
	}
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)
	for _, key := range req.Keys {
		r, err := commitOne(key, txn, resp, req.StartVersion, req.CommitVersion)
		if r != nil || err != nil {
			return r, err
		}
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionError, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionError.RequestErr
			return resp, err
		}
		return nil, err
	}
	return resp, nil
}

func commitOne(key []byte, txn *mvcc.MvccTxn, resp *kvrpcpb.CommitResponse, start, commit uint64) (*kvrpcpb.CommitResponse, error) {
	lock, err := txn.GetLock(key)
	if err != nil {
		if regionError, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionError.RequestErr
			return resp, err
		}
		return nil, err
	}
	if lock == nil || lock.Ts != txn.StartTS {
		write, commitTs, err := txn.MostRecentWrite(key)
		if err != nil {
			if regionError, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionError.RequestErr
				return resp, err
			}
			return nil, err
		}
		// 已经提交
		if write != nil && write.Kind != mvcc.WriteKindRollback || commitTs > commitTs {
			return resp, nil
		}
		resp.Error = &kvrpcpb.KeyError{Retryable: "lock not found"}
		return resp, nil
	}
	write := mvcc.Write{
		StartTS: start,
		Kind:    lock.Kind,
	}
	txn.PutWrite(key, commit, &write)
	txn.DeleteLock(key)
	return nil, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	resp := new(kvrpcpb.ScanResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionError, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionError.RequestErr
			return resp, err
		}
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.Version)

	scanner := mvcc.NewScanner(req.StartKey, txn)
	defer scanner.Close()
	limit := req.Limit
	for {
		if limit == 0 {
			return resp, nil
		}
		limit -= 1

		key, value, err := scanner.Next()
		if err != nil {
			// scan 遇到错误不能直接返回，还是要遍历完
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
			// Reached the end of the DB
			return resp, nil
		}

		pair := kvrpcpb.KvPair{}
		pair.Key = key
		pair.Value = value
		resp.Pairs = append(resp.Pairs, &pair)
	}
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	key := req.PrimaryKey
	resp := new(kvrpcpb.CheckTxnStatusResponse)
	if len(req.PrimaryKey) == 0 {
		return resp, nil
	}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionError, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionError.RequestErr
			return resp, err
		}
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.LockTs)

	lock, err := txn.GetLock(key)
	if err != nil {
		if regionError, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionError.RequestErr
			return resp, err
		}
		return nil, err
	}
	// 如果存在该事务的锁
	if lock != nil && lock.Ts == txn.StartTS {
		// 如果锁过期了，那么这个锁就可以通过CheckTxnStatus这样的并发命令回滚
		if mvcc.PhysicalTime(lock.Ts)+lock.Ttl < mvcc.PhysicalTime(req.CurrentTs) {
			// YOUR CODE HERE (lab1).
			// Lock has expired, try to rollback it. `mvcc.WriteKindRollback` could be used to
			// represent the type. Try using the interfaces provided by `mvcc.MvccTxn`.
			resp.Action = kvrpcpb.Action_TTLExpireRollback
			if lock.Kind == mvcc.WriteKindPut {
				txn.DeleteValue(key)
			}
			txn.DeleteLock(key)
			write := &mvcc.Write{StartTS: txn.StartTS, Kind: mvcc.WriteKindRollback}
			txn.PutWrite(key, txn.StartTS, write)

			err = server.storage.Write(req.Context, txn.Writes())
			if err != nil {
				if regionError, ok := err.(*raft_storage.RegionError); ok {
					resp.RegionError = regionError.RequestErr
					return resp, err
				}
				return nil, err
			}
		} else {
			// Lock has not expired, leave it alone.
			resp.Action = kvrpcpb.Action_NoAction
			resp.LockTtl = lock.Ttl
		}

		return resp, nil
	}

	existingWrite, commitTs, err := txn.CurrentWrite(key)
	if err != nil {
		if regionError, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionError.RequestErr
			return resp, err
		}
		return nil, err
	}
	if existingWrite == nil {
		//锁从未存在过，仍然需要在它上面放一个回滚记录，这样旧的事务命令，
		//如对key的prewrite就会失败。注意尝试设置正确的`response.Action`。行动类型可以在kvrpcpb.Action_xxx中找到。
		// 为什么呢？因为这行没有锁，且从来没有提交过，所以应该防止之后旧的事务再来要锁成功。
		write := &mvcc.Write{StartTS: txn.StartTS, Kind: mvcc.WriteKindRollback}
		txn.PutWrite(key, txn.StartTS, write)
		resp.Action = kvrpcpb.Action_LockNotExistRollback
		err = server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			if regionError, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionError.RequestErr
				return resp, err
			}
			return nil, err
		}
		return resp, nil
	}

	if existingWrite.Kind == mvcc.WriteKindRollback {
		// The key has already been rolled back, so nothing to do.
		resp.Action = kvrpcpb.Action_NoAction
		return resp, nil
	}

	// The key has already been committed.
	resp.CommitVersion = commitTs
	resp.Action = kvrpcpb.Action_NoAction
	return resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.BatchRollbackResponse{}
	if len(req.Keys) == 0 {
		return resp, nil
	}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionError, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionError.RequestErr
			return resp, err
		}
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for i := range req.Keys {
		r, err := rollbackOne(req.Keys[i], txn, resp)
		if r != nil || err != nil {
			return r, err
		}
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionError, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionError.RequestErr
			return resp, err
		}
		return nil, err
	}
	return resp, nil
}

func rollbackOne(key []byte, txn *mvcc.MvccTxn, resp *kvrpcpb.BatchRollbackResponse) (*kvrpcpb.BatchRollbackResponse, error) {
	lock, err := txn.GetLock(key)
	if err != nil {
		if regionError, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionError.RequestErr
			return resp, err
		}
		return nil, err
	}

	//panic("rollbackKey is not implemented yet")
	// 如果锁是空的，且不是当前Ts的事务，就需要去write列找信息
	if lock == nil || lock.Ts != txn.StartTS {
		// There is no lock, check the write status.
		existingWrite, ts, err := txn.CurrentWrite(key)
		if err != nil {
			if regionError, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionError.RequestErr
				return resp, err
			}
			return nil, err
		}
		// 如果没有对应的记录，尝试插入一条回滚记录，使用`mvcc.WriteKindRollback`来表示该类型。
		// 另外，命令可能是陈旧的，即该记录已经回滚或提交。
		// 如果也没有写，估计是预写丢失了。无论如何我们都要插入一个回滚写入。
		// 如果键已经被回滚了，那么就没什么可做的。
		// 如果键已经被提交了。 这不应该发生，因为客户端不应该同时发送提交和回滚请求。
		// 也没有写，估计是预写丢失了。我们还是插入一个回滚写入。
		if existingWrite == nil {
			write := &mvcc.Write{StartTS: txn.StartTS, Kind: mvcc.WriteKindRollback}
			txn.PutWrite(key, txn.StartTS, write)
			return nil, nil
		} else {
			if existingWrite.Kind == mvcc.WriteKindRollback {
				// The key has already been rolled back, so nothing to do.
				return nil, nil
			}

			// The key has already been committed. This should not happen since the client should never send both
			// commit and rollback requests.
			err := new(kvrpcpb.KeyError)
			err.Abort = fmt.Sprintf("key has already been committed: %v at %d", key, ts)
			resp.Error = err
			return resp, nil
		}
	}
	// 如果锁还存在且是写入，就需要回滚值
	if lock.Kind == mvcc.WriteKindPut {
		txn.DeleteValue(key)
	}
	// 写入回滚记录并删除锁
	write := mvcc.Write{StartTS: txn.StartTS, Kind: mvcc.WriteKindRollback}
	txn.PutWrite(key, txn.StartTS, &write)
	txn.DeleteLock(key)

	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	// 一个从开始时间戳到提交时间戳的映射，
	// 它告诉我们一个事务（由开始时间戳识别）是否已经提交（如果是，那么为提交时间戳）或回滚（在这种情况下提交时间戳是0）。
	resp := &kvrpcpb.ResolveLockResponse{}
	if req.StartVersion == 0 {
		return resp, nil
	}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()
	iter := reader.IterCF(engine_util.CfLock)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer iter.Close()
	// 寻找ts为startversion的lock的all keys
	var keys [][]byte
	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		value, err := item.ValueCopy(nil)
		if err != nil {
			return resp, err
		}
		lock, err := mvcc.ParseLock(value)
		if err != nil {
			return resp, err
		}
		if lock.Ts == req.StartVersion {
			key := item.KeyCopy(nil)
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		return resp, nil
	}
	// commitTs为0则回滚
	if req.CommitVersion == 0 {
		r, err := server.KvBatchRollback(nil, &kvrpcpb.BatchRollbackRequest{
			Context:      nil,
			StartVersion: req.StartVersion,
			Keys:         keys,
		})
		resp.Error = r.Error
		resp.RegionError = r.RegionError
		return resp, err
	} else {
		r, err := server.KvCommit(nil, &kvrpcpb.CommitRequest{
			Context:       nil,
			StartVersion:  req.StartVersion,
			Keys:          keys,
			CommitVersion: req.CommitVersion,
		})
		resp.Error = r.Error
		resp.RegionError = r.RegionError
		return resp, err
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
