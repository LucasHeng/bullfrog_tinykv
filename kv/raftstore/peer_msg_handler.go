package raftstore

import (
	"context"
	"fmt"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/raft"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	if !d.RaftGroup.HasReady() {
		return
	}

	rd := d.RaftGroup.Ready()

	// 处理ready,保存未持久的log然后应用commit
	result, err := d.peerStorage.SaveReadyState(&rd)

	if err != nil {
		// error处理
		log.Debugf("exist err:%v", err)
		return
	}

	if result != nil && result.Region != nil {
		d.peerStorage.SetRegion(result.Region)
		d.ctx.storeMeta.Lock()
		// log.Infof("prevregion:%v region:%v", result.PrevRegion, result.Region)
		// d.ctx.storeMeta.regionRanges.Delete(&regionItem{result.PrevRegion})
		d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: result.Region})
		d.ctx.storeMeta.regions[d.regionId] = d.Region()
		d.ctx.storeMeta.Unlock()
	}

	if rd.SoftState != nil {
		// ss应该怎么弄？
		// 如果目前软状态为leader，那么应该和scheduler通气
		if rd.SoftState.RaftState == raft.StateLeader {
			d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
		}
	}

	// 处理messages
	if len(rd.Messages) != 0 {
		d.Send(d.ctx.trans, rd.Messages)
	}

	// 处理committed entries
	if len(rd.CommittedEntries) != 0 {
		err := d.HandleCommittedEntries(rd.CommittedEntries)
		if err != nil {
			log.Errorf("%v", err)
			panic(err)
		}
	}

	// log.Infof("Node:%d Ready apply successfully raftstate:%v and applystate:%v", d.PeerId(), d.peerStorage.raftState, d.peerStorage.applyState)

	d.RaftGroup.Advance(rd)
	// Your Code Here (2B).
}

func PanicTest() {

}

func (d *peerMsgHandler) HandleCommittedEntries(committedEntries []eraftpb.Entry) error {
	kvWB := &engine_util.WriteBatch{}
	for _, e := range committedEntries {
		switch e.EntryType {
		case eraftpb.EntryType_EntryNormal:
			d.HandleEntry(&e, kvWB)
		case eraftpb.EntryType_EntryConfChange:
			d.HandleConfchange(&e, kvWB)
		default:
			panic("no exist entrytype")
		}
		// d.HandleEntry(&e, kvWB)
		if d.stopped {
			return nil
		}
	}
	d.peerStorage.applyState.AppliedIndex = committedEntries[len(committedEntries)-1].Index
	kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
	err := kvWB.WriteToDB(d.peerStorage.Engines.Kv)
	return err
}

func (d *peerMsgHandler) FindProposal(e *eraftpb.Entry) (*proposal, bool) {
	if len(d.proposals) != 0 {
		proposal := d.proposals[0]
		// 过时的cmd的propsal都要扔掉，回复err，提醒client再发cmd
		for proposal.index < e.Index {
			proposal.cb.Done(ErrResp(&util.ErrStaleCommand{}))
			d.proposals = d.proposals[1:]
			if len(d.proposals) == 0 {
				// 没有proposal,结束这个
				return nil, false
			}
			proposal = d.proposals[0]
		}
		if proposal.index == e.Index {
			if proposal.term != e.Term {
				// term变了，证明是下一个轮回了，那么这个请求就应该扔掉
				NotifyStaleReq(e.Index, proposal.cb)
				d.proposals = d.proposals[1:]
				return nil, false
			}

			return proposal, true
		}
	}
	return nil, false
}

func (d *peerMsgHandler) getKeyFromReq(req *raft_cmdpb.Request) []byte {
	var key []byte
	switch req.CmdType {
	case raft_cmdpb.CmdType_Get:
		key = req.Get.Key
	case raft_cmdpb.CmdType_Put:
		key = req.Put.Key
	case raft_cmdpb.CmdType_Delete:
		key = req.Delete.Key
	default:
		key = nil
	}
	return key
}

func (d *peerMsgHandler) HandleEntry(e *eraftpb.Entry, kvWB *engine_util.WriteBatch) {
	// 解码出raftcmd
	msg := &raft_cmdpb.RaftCmdRequest{}
	err := msg.Unmarshal(e.Data)
	if err != nil {
		panic(err)
	}
	if len(msg.Requests) != 0 {
		resp := &raft_cmdpb.RaftCmdResponse{Header: &raft_cmdpb.RaftResponseHeader{}}
		proposal, ok := d.FindProposal(e)
		for _, req := range msg.Requests {
			// 检查这个key是否在当前region，没有就返回错误
			if key := d.getKeyFromReq(req); key != nil {
				err := util.CheckKeyInRegion(key, d.Region())
				if err != nil {
					if ok {
						proposal.cb.Done(ErrResp(err))
					}
					return
				}
			}
			switch req.CmdType {
			case raft_cmdpb.CmdType_Invalid:
			case raft_cmdpb.CmdType_Get:
				// get应该返回当前值
				if !ok {
					continue
				}
				d.peerStorage.applyState.AppliedIndex = e.Index
				kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
				kvWB.WriteToDB(d.peerStorage.Engines.Kv)
				kvWB.Reset()
				val, _ := engine_util.GetCF(d.peerStorage.Engines.Kv, req.Get.Cf, req.Get.Key)
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Get,
					Get:     &raft_cmdpb.GetResponse{Value: val},
				})
			case raft_cmdpb.CmdType_Put:
				kvWB.SetCF(req.Put.Cf, req.Put.GetKey(), req.Put.GetValue())
				if !ok {
					continue
				}
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Put,
					Put:     &raft_cmdpb.PutResponse{},
				})
			case raft_cmdpb.CmdType_Delete:
				kvWB.DeleteCF(req.Delete.Cf, req.Delete.GetKey())
				if !ok {
					continue
				}
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Delete,
					Delete:  &raft_cmdpb.DeleteResponse{},
				})
			case raft_cmdpb.CmdType_Snap:
				if !ok {
					continue
				}
				d.peerStorage.applyState.AppliedIndex = e.Index
				kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
				kvWB.WriteToDB(d.peerStorage.Engines.Kv)
				kvWB.Reset()
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Snap,
					Snap:    &raft_cmdpb.SnapResponse{Region: d.Region()},
				})
				// resp.Responses = []*raft_cmdpb.Response{{CmdType: raft_cmdpb.CmdType_Snap, Snap: &raft_cmdpb.SnapResponse{Region: d.Region()}}}
				proposal.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
			}

			//	// 回复
			//	if len(d.proposals) != 0 {
			//		proposal := d.proposals[0]
			//		// 过时的cmd的propsal都要扔掉，回复err，提醒client再发cmd
			//		for proposal.index < e.Index {
			//			proposal.cb.Done(ErrResp(&util.ErrStaleCommand{}))
			//			d.proposals = d.proposals[1:]
			//			if len(d.proposals) == 0 {
			//				// 没有proposal,结束这个
			//				return
			//			}
			//			proposal = d.proposals[0]
			//		}
			//		if proposal.index == e.Index {
			//			if proposal.term != e.Term {
			//				// term变了，证明是下一个轮回了，那么这个请求就应该扔掉
			//				NotifyStaleReq(e.Index, proposal.cb)
			//				d.proposals = d.proposals[1:]
			//				return
			//			}

			//			switch req.CmdType {
			//			case raft_cmdpb.CmdType_Invalid:
			//			case raft_cmdpb.CmdType_Get:
			//				// get应该返回当前值
			//				d.peerStorage.applyState.AppliedIndex = e.Index
			//				kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			//				kvWB.WriteToDB(d.peerStorage.Engines.Kv)
			//				kvWB.Reset()
			//				val, _ := engine_util.GetCF(d.peerStorage.Engines.Kv, req.Get.Cf, req.Get.Key)
			//				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
			//					CmdType: raft_cmdpb.CmdType_Get,
			//					Get:     &raft_cmdpb.GetResponse{Value: val},
			//				})
			//				// resp.Responses = []*raft_cmdpb.Response{{CmdType: raft_cmdpb.CmdType_Get, Get: &raft_cmdpb.GetResponse{
			//				//  	Value: val,
			//				// }}}
			//			case raft_cmdpb.CmdType_Put:
			//				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
			//					CmdType: raft_cmdpb.CmdType_Put,
			//					Put:     &raft_cmdpb.PutResponse{},
			//				})
			//				// resp.Responses = []*raft_cmdpb.Response{{CmdType: raft_cmdpb.CmdType_Put, Put: &raft_cmdpb.PutResponse{}}}
			//			case raft_cmdpb.CmdType_Delete:
			//				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
			//					CmdType: raft_cmdpb.CmdType_Delete,
			//					Delete:  &raft_cmdpb.DeleteResponse{},
			//				})
			//				// resp.Responses = []*raft_cmdpb.Response{{CmdType: raft_cmdpb.CmdType_Delete, Delete: &raft_cmdpb.DeleteResponse{}}}
			//			case raft_cmdpb.CmdType_Snap:
			//				d.peerStorage.applyState.AppliedIndex = e.Index
			//				kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			//				kvWB.WriteToDB(d.peerStorage.Engines.Kv)
			//				kvWB.Reset()
			//				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
			//					CmdType: raft_cmdpb.CmdType_Snap,
			//					Snap:    &raft_cmdpb.SnapResponse{Region: d.Region()},
			//				})
			//				// resp.Responses = []*raft_cmdpb.Response{{CmdType: raft_cmdpb.CmdType_Snap, Snap: &raft_cmdpb.SnapResponse{Region: d.Region()}}}
			//				proposal.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
			//			}
			//			// // 对于request只有一个的情况
			//			// d.peerStorage.Engines.Kproposal.cb.Done(resp)
			//			// d.proposals = d.proposals[1:]
			//		}
			//	}
		}
		if ok {
			proposal.cb.Done(resp)
			d.proposals = d.proposals[1:]
		}
		// // d.proposals[0].cb.Done(resp)
		// 先应用了，回复看是否有proposal
		// if len(d.proposals) != 0 {
		// 	proposal := d.proposals[0]
		// 	// 过时的cmd的propsal都要扔掉，回复err，提醒client再发cmd
		// 	for proposal.index < e.Index {
		// 		proposal.cb.Done(ErrResp(&util.ErrStaleCommand{}))
		// 		d.proposals = d.proposals[1:]
		// 		if len(d.proposals) == 0 {
		// 			// 没有proposal,结束这个
		// 			return
		// 		}
		// 		proposal = d.proposals[0]
		// 	}
		// 	if proposal.index == e.Index {
		// 		if proposal.term != e.Term {
		// 			// term变了，证明是下一个轮回了，那么这个请求就应该扔掉
		// 			NotifyStaleReq(e.Index, proposal.cb)
		// 			d.proposals = d.proposals[1:]
		// 			return
		// 		}
		// 		proposal.cb.Done(resp)
		// 		if hassnap {
		// 			proposal.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
		// 		}
		// 		d.proposals = d.proposals[1:]
		// 	}
		// }
	}
	// 可能是都有，所以不能else if
	if msg.AdminRequest != nil {
		areq := msg.GetAdminRequest()
		switch areq.CmdType {
		case raft_cmdpb.AdminCmdType_CompactLog:
			log := areq.GetCompactLog()
			// 修改applystate的trun
			if log.GetCompactIndex() >= d.peerStorage.truncatedIndex() {
				d.peerStorage.applyState.TruncatedState.Index = log.GetCompactIndex()
				d.peerStorage.applyState.TruncatedState.Term = log.GetCompactTerm()
				kvWB.SetMeta(meta.ApplyStateKey(d.Region().Id), d.peerStorage.applyState)
				d.ScheduleCompactLog(log.GetCompactIndex())
			}
		case raft_cmdpb.AdminCmdType_Split:
			// 判断版本，版本不对，那么就扔掉
			proposal, exist := d.FindProposal(e)

			region := d.Region()
			if err := util.CheckRegionEpoch(msg, region, true); err != nil {
				if err, ok := err.(*util.ErrEpochNotMatch); ok {
					// 如果regionepoch不对，那么后续就不执行了
					if exist {
						proposal.cb.Done(ErrResp(err))
						d.proposals = d.proposals[1:]
					}
					return
				}
			}

			// 避免传错region
			if d.regionId != msg.Header.RegionId {
				err := &util.ErrRegionNotFound{RegionId: msg.Header.RegionId}
				if exist {
					proposal.cb.Done(ErrResp(err))
					d.proposals = d.proposals[1:]
				}
				return
			}

			// 检查key是否在region
			if err := util.CheckKeyInRegion(areq.Split.SplitKey, d.Region()); err != nil {
				if exist {
					proposal.cb.Done(ErrResp(err))
					d.proposals = d.proposals[1:]
				}
				return
			}

			// 切分后的regions的peers
			newpeers := make([]*metapb.Peer, 0)
			for i, peer := range region.Peers {
				newpeers = append(newpeers, &metapb.Peer{
					Id:      areq.Split.NewPeerIds[i],
					StoreId: peer.StoreId,
				})
			}

			// 创建新的region
			// 新的region是在原region的基础上往后切
			newregion := &metapb.Region{
				Id:       areq.Split.NewRegionId,
				StartKey: areq.Split.SplitKey,
				EndKey:   region.EndKey,
				Peers:    newpeers,
				RegionEpoch: &metapb.RegionEpoch{
					ConfVer: 1,
					Version: 1,
				},
			}

			// 修改storemeta
			storeMeta := d.ctx.storeMeta
			storeMeta.Lock()
			// 修改原来region的信息
			d.Region().EndKey = areq.Split.SplitKey
			d.Region().RegionEpoch.Version++

			// 修改上下文中的全局本region和新region
			// 因为是全局上下文，所以leader修改，所有的region信息都会同步
			storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: d.Region()})
			storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: newregion})

			//修改regions
			storeMeta.regions[newregion.Id] = newregion
			storeMeta.Unlock()

			// region信息持久化
			meta.WriteRegionState(kvWB, d.Region(), rspb.PeerState_Normal)
			meta.WriteRegionState(kvWB, newregion, rspb.PeerState_Normal)

			// 创建peer
			peer, err := createPeer(d.ctx.store.Id, d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newregion)
			if err != nil {
				panic(err)
			}

			// 注册新的peer
			d.ctx.router.register(peer)
			// 启动peer
			d.ctx.router.send(newregion.Id, message.NewMsg(message.MsgTypeStart, nil))
			// 回调返回，分裂成功
			if exist {
				resp := &raft_cmdpb.RaftCmdResponse{
					Header: &raft_cmdpb.RaftResponseHeader{},
					AdminResponse: &raft_cmdpb.AdminResponse{
						CmdType: raft_cmdpb.AdminCmdType_Split,
						Split: &raft_cmdpb.SplitResponse{
							Regions: []*metapb.Region{region, newregion},
						},
					},
				}
				proposal.cb.Done(resp)
				d.proposals = d.proposals[1:]
			}
		default:
			panic("unexist admincmd")
		}
	}
}

func hasPeer(region *metapb.Region, id uint64) bool {
	for _, p := range region.GetPeers() {
		if p.Id == id {
			return true
		}
	}
	return false
}

func (d *peerMsgHandler) HandleConfchange(e *eraftpb.Entry, kvWB *engine_util.WriteBatch) {
	var cc eraftpb.ConfChange
	err := cc.Unmarshal(e.Data)
	if err != nil {
		panic(err)
	}
	msg := &raft_cmdpb.RaftCmdRequest{}
	err = msg.Unmarshal(cc.Context)
	if err != nil {
		panic(err)
	}

	// 判断版本，版本不对，那么就扔掉
	region := d.Region()
	if err := util.CheckRegionEpoch(msg, region, true); err != nil {
		if err, ok := err.(*util.ErrEpochNotMatch); ok {
			// 如果regionepoch不对，那么后续就不执行了
			if len(d.proposals) > 0 {
				proposal, ok := d.FindProposal(e)
				if ok {
					proposal.cb.Done(ErrResp(err))
					d.proposals = d.proposals[1:]
					return
				}
			}
		}
		return
	}
	//
	switch cc.ChangeType {
	case eraftpb.ConfChangeType_AddNode:
		// 没有就加上
		if !hasPeer(d.Region(), cc.GetNodeId()) {
			d.Region().RegionEpoch.ConfVer++
			peer := &metapb.Peer{
				Id:      cc.GetNodeId(),
				StoreId: msg.AdminRequest.ChangePeer.Peer.StoreId,
			}
			d.Region().Peers = append(d.Region().Peers, peer)
			meta.WriteRegionState(kvWB, d.Region(), rspb.PeerState_Normal)
			storeMeta := d.ctx.storeMeta
			storeMeta.Lock()
			storeMeta.regions[d.Region().GetId()] = d.Region()
			storeMeta.Unlock()
			d.insertPeerCache(peer)
		}
	case eraftpb.ConfChangeType_RemoveNode:
		// 要删除的节点是自己
		if cc.NodeId == d.PeerId() {
			// if len(region.Peers) == 2 && d.IsLeader() {
			// 	// var to uint64
			// 	// for _, p := range region.Peers {
			// 	// 	if p.Id != cc.NodeId {
			// 	// 		to = p.Id
			// 	// 	}
			// 	// }
			// 	// if to != 0 {
			// 	// 	log.Infof("Node:%d transferleader to %d", d.PeerId(), to)
			// 	// 	d.RaftGroup.TransferLeader(to)
			// 	// }
			// 	// // 等待 transfer success
			// 	// return
			// 	proposal, ok := d.FindProposal(e)
			// 	if !ok {
			// 		return
			// 	}
			// 	err := fmt.Sprintf("%s return corner case\n", d.Tag)
			// 	proposal.cb.Done(ErrResp(errors.New(err)))
			// 	return
			// }
			d.destroyPeer()
			return
		}

		if hasPeer(d.Region(), cc.GetNodeId()) {
			d.Region().RegionEpoch.ConfVer++
			for i, p := range d.Region().GetPeers() {
				if p.Id == cc.GetNodeId() {
					d.Region().Peers = append(d.Region().Peers[:i], d.Region().Peers[i+1:]...)
					break
				}
			}
			// region信息改变，持久化
			meta.WriteRegionState(kvWB, d.Region(), rspb.PeerState_Normal)
			// 跟新globalcontext里的storeMeta
			storeMeta := d.ctx.storeMeta
			storeMeta.Lock()
			storeMeta.regions[d.Region().Id] = d.Region()
			storeMeta.Unlock()
			// 修改peer Cache
			d.removePeerCache(cc.NodeId)
		}
	}

	// 幂等的，所以可以多次调用?
	d.RaftGroup.ApplyConfChange(cc)

	// 更新scheduler中的信息
	if d.IsLeader() {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}

	proposal, ok := d.FindProposal(e)
	resp := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: &raft_cmdpb.AdminResponse{
			CmdType:    raft_cmdpb.AdminCmdType_ChangePeer,
			ChangePeer: &raft_cmdpb.ChangePeerResponse{Region: d.Region()},
		},
	}
	if ok {
		proposal.cb.Done(resp)
		d.proposals = d.proposals[1:]
		return
	}
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
		// log.Infof("cmd:%v", raftCMD)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		log.Infof("Node:%v state:%v leaderid:%v leader:%v peercache:%v  peers:%v prs:%v", d.Tag, d.peer.RaftGroup.Raft.State.String(), leaderID, leader, d.peerCache, d.peerStorage.Region().Peers, d.peer.RaftGroup.Raft.Prs)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
	// 放入回调
	if msg.Requests != nil {
		d.proposals = append(d.proposals, &proposal{
			index: d.nextProposalIndex(),
			term:  d.Term(),
			cb:    cb,
		})
		// log.Infof("msg:%v", msg)
		// 往raftnode发propose
		data, err := msg.Marshal()
		if err != nil {
			panic(err)
		}
		d.RaftGroup.Propose(data)
	} else if msg.AdminRequest != nil {
		d.proposeAdminRequest(msg, cb)
	}
}

func (d *peerMsgHandler) proposeAdminRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	switch msg.AdminRequest.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		data, err := msg.Marshal()
		if err != nil {
			panic(err)
		}
		d.RaftGroup.Propose(data)
	case raft_cmdpb.AdminCmdType_TransferLeader:
		// 通过rawnode执行transferleader
		resp := &raft_cmdpb.RaftCmdResponse{
			Header:        &raft_cmdpb.RaftResponseHeader{},
			AdminResponse: &raft_cmdpb.AdminResponse{CmdType: raft_cmdpb.AdminCmdType_TransferLeader},
		}
		d.RaftGroup.TransferLeader(msg.AdminRequest.TransferLeader.Peer.Id)
		cb.Done(resp)
	case raft_cmdpb.AdminCmdType_ChangePeer:
		// 这个也需要进行raft复制
		// 保证每时每刻只有一个confchange
		if d.RaftGroup.Raft.PendingConfIndex > d.peerStorage.AppliedIndex() {
			return
		}
		if (msg.AdminRequest.ChangePeer.ChangeType == eraftpb.ConfChangeType_RemoveNode) && d.IsLeader() && len(d.peerCache) == 2 && msg.AdminRequest.ChangePeer.Peer.Id == d.PeerId() {
			return
		}
		d.proposals = append(d.proposals, &proposal{
			index: d.nextProposalIndex(),
			term:  d.Term(),
			cb:    cb,
		})
		data, err := msg.Marshal()
		if err != nil {
			panic(err)
		}
		d.RaftGroup.ProposeConfChange(eraftpb.ConfChange{
			ChangeType: msg.AdminRequest.ChangePeer.ChangeType,
			NodeId:     msg.AdminRequest.ChangePeer.Peer.Id,
			Context:    data,
		})
	case raft_cmdpb.AdminCmdType_Split:
		if err := util.CheckKeyInRegion(msg.AdminRequest.Split.SplitKey, d.Region()); err != nil {
			cb.Done(ErrResp(err))
			return
		}
		d.proposals = append(d.proposals, &proposal{
			index: d.nextProposalIndex(),
			term:  d.Term(),
			cb:    cb,
		})
		data, _ := msg.Marshal()
		d.RaftGroup.Propose(data)
	default:
		panic("no such admin request")
	}
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) syncRegion() {
	newregion, _, err := d.ctx.schedulerClient.GetRegionByID(context.TODO(), d.Region().Id)
	region := d.Region()
	if err != nil {
		return
	}
	// 这里只进行
	if util.RegionEqual(newregion, region) {
		return
	}
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

/// Checks if the message is sent to the correct peer.
///
/// Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	// store里面删除regionid的peer，store不会再发送给peer消息
	d.ctx.router.close(regionID)
	d.stopped = true
	// store维护的关于region的B tree，进行修改
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	// store的regions的map的region删除
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	// leader才检查是否需要split
	if !d.IsLeader() {
		return
	}

	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
