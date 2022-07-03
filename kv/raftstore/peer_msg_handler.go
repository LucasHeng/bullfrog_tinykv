package raftstore

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/raft"
	"reflect"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
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

//todo: raft implement have problem
func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	if !d.RaftGroup.HasReady() {
		return
	}
	rd := d.RaftGroup.Ready()
	// 持久化entries和hard state
	res, err := d.peerStorage.SaveReadyState(&rd)
	if err != nil {
		panic(err)
	}
	// 判断 region是否一样，否则就要更换 region
	if res != nil && !reflect.DeepEqual(res.PrevRegion, res.Region) {
		d.SetRegion(res.Region)
		metaStore := d.ctx.storeMeta
		metaStore.Lock()
		metaStore.regions[res.Region.Id] = res.Region
		metaStore.regionRanges.Delete(&regionItem{res.PrevRegion})
		metaStore.regionRanges.ReplaceOrInsert(&regionItem{res.Region})
		metaStore.Unlock()
	}
	// 如果 messages 不为空，就需要发送
	if len(rd.Messages) != 0 {
		d.Send(d.ctx.trans, rd.Messages)
	}
	// apply committed log
	if len(rd.CommittedEntries) > 0 {
		//fmt.Println(rd.CommittedEntries)
		logWb := &engine_util.WriteBatch{}
		for _, ent := range rd.CommittedEntries {
			if ent.EntryType == eraftpb.EntryType_EntryConfChange {
				logWb = d.handleConfChange(ent, logWb)
			} else {
				msg := &raft_cmdpb.RaftCmdRequest{}
				err := msg.Unmarshal(ent.Data)
				if err != nil {
					log.Fatal("msg unmarshal error:", err)
				}
				if len(msg.Requests) > 0 {
					logWb = d.handleRequests(msg, &ent, logWb)

				}
				// 处理像 compact .... 这些的 admin request，然后一起批量写入
				if msg.AdminRequest != nil {
					logWb = d.handleAdminRequests(msg, ent, logWb)
				}
			}
			if d.stopped {
				return
			}
		}
		d.peerStorage.applyState.AppliedIndex = rd.CommittedEntries[len(rd.CommittedEntries)-1].Index
		logWb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
		logWb.WriteToDB(d.peerStorage.Engines.Kv)
		//fmt.Println(rd.CommittedEntries)
	}
	d.RaftGroup.Advance(rd)
}

func (d *peerMsgHandler) processNormalRequest(entry eraftpb.Entry, msg *raft_cmdpb.RaftCmdRequest, wb *engine_util.WriteBatch) *engine_util.WriteBatch {
	req := msg.Requests[0]
	key := getRequestKey(msg.Requests[0])
	if key != nil {
		err := util.CheckKeyInRegion(key, d.Region())
		if err != nil {
			d.handleProposal(entry, func(p *proposal) {
				p.cb.Done(ErrResp(err))
			})
			return wb
		}
	}
	switch req.CmdType {
	case raft_cmdpb.CmdType_Put:
		wb.SetCF(req.Put.Cf, req.Put.Key, req.Put.Value)
		log.Debugf("%s set cf %s-%s-%s", d.Tag, req.Put.Cf, req.Put.Key, req.Put.Value)
	case raft_cmdpb.CmdType_Delete:
		wb.DeleteCF(req.Delete.Cf, req.Delete.Key)
		log.Debugf("%s delete cf %s-%s", d.Tag, req.Delete.Cf, req.Delete.Key)
	}
	d.handleProposal(entry, func(p *proposal) {
		rsp := &raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{},
		}
		switch req.CmdType {
		case raft_cmdpb.CmdType_Get:
			d.peerStorage.applyState.AppliedIndex = entry.Index
			wb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			wb.WriteToDB(d.peerStorage.Engines.Kv)
			value, err := engine_util.GetCF(d.ctx.engine.Kv, req.Get.Cf, req.Get.Key)
			log.Debugf("%s get cf %s-%s-%s", d.Tag, req.Get.Cf, req.Get.Key, value)
			if err != nil {
				value = nil
			}
			rsp.Responses = []*raft_cmdpb.Response{
				{
					CmdType: raft_cmdpb.CmdType_Get,
					Get:     &raft_cmdpb.GetResponse{Value: value},
				},
			}
			wb = &engine_util.WriteBatch{}
		case raft_cmdpb.CmdType_Put:
			rsp.Responses = []*raft_cmdpb.Response{
				{
					CmdType: raft_cmdpb.CmdType_Put,
					Put:     &raft_cmdpb.PutResponse{},
				},
			}
		case raft_cmdpb.CmdType_Delete:
			rsp.Responses = []*raft_cmdpb.Response{
				{
					CmdType: raft_cmdpb.CmdType_Delete,
					Delete:  &raft_cmdpb.DeleteResponse{},
				},
			}
		case raft_cmdpb.CmdType_Snap:
			log.Debugf("%s get snap", d.Tag)
			if msg.Header.RegionEpoch.Version != d.Region().RegionEpoch.Version {
				p.cb.Done(ErrResp(&util.ErrEpochNotMatch{}))
				return
			}
			d.peerStorage.applyState.AppliedIndex = entry.Index
			wb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			wb.WriteToDB(d.ctx.engine.Kv)
			wb = &engine_util.WriteBatch{}
			rsp.Responses = []*raft_cmdpb.Response{
				{
					CmdType: raft_cmdpb.CmdType_Snap,
					Snap:    &raft_cmdpb.SnapResponse{Region: d.Region()},
				},
			}
			// new transaction
			p.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
		}
		p.cb.Done(rsp)
	})
	return wb
}
func (d *peerMsgHandler) handleProposal(entry eraftpb.Entry, handler func(*proposal)) {
	if len(d.proposals) > 0 {
		p := d.proposals[0]
		if p.index == entry.Index {
			if p.term != entry.Term {
				NotifyStaleReq(entry.Term, p.cb)
			} else {
				handler(p)
			}
		}
		// handle one proposal
		d.proposals = d.proposals[1:]
	}
}

func regionPeerIndex(region *metapb.Region, id uint64) int {
	for i, peer := range region.Peers {
		if peer.Id == id {
			return i
		}
	}
	return -1
}
func (d *peerMsgHandler) handleConfChange(ent eraftpb.Entry, wb *engine_util.WriteBatch) *engine_util.WriteBatch {
	cc := &eraftpb.ConfChange{}
	err := cc.Unmarshal(ent.Data)
	if err != nil {
		panic(err)
	}
	msg := &raft_cmdpb.RaftCmdRequest{}
	err = msg.Unmarshal(cc.Context)
	if err != nil {
		panic(err)
	}
	//region := d.Region()
	//if err, ok := util.CheckRegionEpoch(msg, region, true).(*util.ErrEpochNotMatch); ok {
	//	p := d.findProposal(&ent)
	//	if p != nil {
	//		p.cb.Done(ErrResp(err))
	//	}
	//	return wb
	//}
	d.RaftGroup.ApplyConfChange(*cc)
	peerIndex := -1
	for i, peer := range d.Region().Peers {
		if peer.Id == cc.NodeId {
			peerIndex = i
		}
	}
	switch cc.ChangeType {
	case eraftpb.ConfChangeType_RemoveNode:
		if cc.NodeId == d.PeerId() {
			if d.IsLeader() && len(d.RaftGroup.Raft.Prs) == 2 {
				p := d.findProposal(&ent)
				if p != nil {
					p.cb.Done(ErrResp(errors.New("corner case")))
				}
				return wb
			}
			d.destroyPeer()
			break
		}
		if peerIndex != -1 {
			d.Region().Peers = append(d.Region().Peers[:peerIndex], d.Region().Peers[peerIndex+1:]...)
			d.Region().RegionEpoch.ConfVer++
			meta.WriteRegionState(wb, d.Region(), rspb.PeerState_Normal)
			//storeMeta := d.ctx.storeMeta
			//storeMeta.Lock()
			//storeMeta.regions[region.Id] = region
			//storeMeta.Unlock()
			meta.WriteRegionState(wb, d.Region(), rspb.PeerState_Normal)
			d.removePeerCache(cc.NodeId)
		}
	case eraftpb.ConfChangeType_AddNode:
		if peerIndex == -1 {
			peer := msg.AdminRequest.ChangePeer.Peer
			d.Region().Peers = append(d.Region().Peers, peer)
			d.Region().RegionEpoch.ConfVer++
			meta.WriteRegionState(wb, d.Region(), rspb.PeerState_Normal)
			//storeMeta := d.ctx.storeMeta
			//storeMeta.Lock()
			//storeMeta.regions[region.Id] = region
			//storeMeta.Unlock()
			//d.insertPeerCache(peer)
		}
	}
	p := d.findProposal(&ent)
	if p != nil {
		p.cb.Done(&raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{},
			AdminResponse: &raft_cmdpb.AdminResponse{
				CmdType:    raft_cmdpb.AdminCmdType_ChangePeer,
				ChangePeer: &raft_cmdpb.ChangePeerResponse{},
			},
		})
	}
	if d.IsLeader() {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return wb
}

func (d *peerMsgHandler) handleAdminRequests(msg *raft_cmdpb.RaftCmdRequest, ent eraftpb.Entry, wb *engine_util.WriteBatch) *engine_util.WriteBatch {
	req := msg.AdminRequest
	switch req.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		compact := req.CompactLog
		applyState := d.peerStorage.applyState
		// update truncated state
		if compact.CompactIndex >= applyState.TruncatedState.Index {
			applyState.TruncatedState.Index = compact.CompactIndex
			applyState.TruncatedState.Term = compact.CompactTerm
			wb.SetMeta(meta.ApplyStateKey(d.regionId), applyState)
			d.ScheduleCompactLog(compact.CompactIndex)
		}
	case raft_cmdpb.AdminCmdType_Split:
		split := req.Split
		p := d.findProposal(&ent)
		raft.SplitPrint("[handle admin split] %v", split)
		// 使用 engine_util.ExceedEndKey() 与 region 的 end key 进行比较
		//exceed := engine_util.ExceedEndKey(split.SplitKey, d.Region().EndKey)
		//if exceed {
		//	raft.SplitPrint("[handle admin split] split key %v exceed, end key %v",split.SplitKey, d.Region().EndKey)
		//	if p != nil {
		//		p.cb.Done(ErrResp(&util.ErrStaleCommand{}))
		//	}
		//	return wb
		//}
		//  consider ErrRegionNotFound & ErrKeyNotInRegion & ErrEpochNotMatch
		if d.regionId != msg.Header.RegionId {
			err := &util.ErrRegionNotFound{RegionId: msg.Header.RegionId}
			if p != nil {
				p.cb.Done(ErrResp(err))
			}
			return wb
		}
		// 检查region epoch
		regionErr, ok := util.CheckRegionEpoch(msg, d.Region(), true).(*util.ErrEpochNotMatch)
		if ok {
			if p != nil {
				p.cb.Done(ErrResp(regionErr))
			}
			return wb
		}
		// 检查key
		if err := util.CheckKeyInRegion(split.SplitKey, d.Region()); err != nil {
			if p != nil {
				// errNotInRegion
				p.cb.Done(ErrResp(err))
			}
			return wb
		}
		// 更新元数据
		//storeMeta := d.ctx.storeMeta
		//storeMeta.Lock()
		//storeMeta.regionRanges.Delete(&regionItem{d.Region()})
		//// 删除 b tree 上的 region
		//meta.regionRanges.Delete(&regionItem{region: d.Region()})
		//d.Region().RegionEpoch.Version += 1
		for i := 0; i < len(d.Region().Peers); i++ {
			for j := 0; j < len(d.Region().Peers)-i-1; j++ {
				if d.Region().Peers[j].Id > d.Region().Peers[j+1].Id {
					//temp := d.Region().Peers[j+1]
					d.Region().Peers[j+1], d.Region().Peers[j] = d.Region().Peers[j], d.Region().Peers[j+1]
					//d.Region().Peers[j] = temp
				}
			}
		}
		newPeers := make([]*metapb.Peer, 0)
		for i, peer := range d.Region().Peers {
			newPeers = append(newPeers, &metapb.Peer{
				Id:      split.NewPeerIds[i],
				StoreId: peer.StoreId,
			})
		}
		// 创建一个新region ， [splitKey, endKey)
		// 当一个 Region 分裂成两个 Region 时，其中一个 Region 将继承分裂前的元数据，
		// 只是修改其 Range 和 RegionEpoch，而另一个将创建相关的元信息
		newRegion := &metapb.Region{
			Id:       split.NewRegionId,
			StartKey: split.SplitKey,
			EndKey:   d.Region().EndKey,
			RegionEpoch: &metapb.RegionEpoch{
				Version: 1,
				ConfVer: 1,
			},
			Peers: newPeers,
		}
		// 这个新创建的 Region 的对应 Peer 应该由 createPeer() 创建，并register到 router.regions。
		// 而 region 的信息应该插入 ctx.StoreMeta 中的regionRanges 中。
		peers, err := createPeer(d.ctx.store.Id, d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
		if err != nil {
			panic(err)
		}
		storeMeta := d.ctx.storeMeta
		storeMeta.Lock()
		d.Region().RegionEpoch.Version++
		d.Region().EndKey = split.SplitKey
		storeMeta.regions[split.NewRegionId] = newRegion
		storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})
		storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: d.Region()})
		storeMeta.Unlock()
		meta.WriteRegionState(wb, d.Region(), rspb.PeerState_Normal)
		meta.WriteRegionState(wb, newRegion, rspb.PeerState_Normal)
		// 让分割检查器先不要来扫描数据，因为刚刚分割完，可能还没创建完成
		d.SizeDiffHint = 0
		d.ApproximateSize = new(uint64)

		d.ctx.router.register(peers)
		m := message.NewMsg(message.MsgTypeStart, nil)
		m.RegionID = split.NewRegionId
		d.ctx.router.send(split.NewRegionId, m)
		if p != nil {
			resp := &raft_cmdpb.RaftCmdResponse{
				Header: &raft_cmdpb.RaftResponseHeader{},
				AdminResponse: &raft_cmdpb.AdminResponse{
					CmdType: req.CmdType,
					Split:   &raft_cmdpb.SplitResponse{Regions: []*metapb.Region{d.Region(), newRegion}},
				},
			}
			p.cb.Done(resp)
		}
		//if d.IsLeader() {
		//	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
		//}
	}
	return wb
}

func (d *peerMsgHandler) handleRequests(msg *raft_cmdpb.RaftCmdRequest, ent *eraftpb.Entry, wb *engine_util.WriteBatch) *engine_util.WriteBatch {
	// 先把请求都写了
	requests := msg.Requests
	for _, req := range requests {
		// 检查他们的 key 是否还在该 region 中，因为在 raftCmd 同步过程中，
		// 可能会发生 region 的 split，也需要检查 RegionEpoch 是否匹配。
		key := getRequestKey(req)
		if key != nil {
			err := util.CheckKeyInRegion(key, d.Region())
			if err != nil {
				p := d.findProposal(ent)
				if p != nil {
					p.cb.Done(ErrResp(err))
				}
				return wb
			}
		}
		raft.ToBPrint("[handle ready] %v handle , type : %v", d.PeerId(), req.CmdType)
		switch req.CmdType {
		case raft_cmdpb.CmdType_Put:
			raft.ToBPrint("[handle ready] %v handle put type , key : %v, value : %v", d.PeerId(), string(req.Put.Key), string(req.Put.Value))
			wb.SetCF(req.Put.Cf, req.Put.Key, req.Put.Value)
		case raft_cmdpb.CmdType_Delete:
			wb.DeleteCF(req.Delete.Cf, req.Delete.Key)
		case raft_cmdpb.CmdType_Snap:
		}
	}
	// 然后依次进行回复
	// 首先找到 propose，看看有没有存在或者合不合法
	if len(d.proposals) > 0 {
		propose := d.findProposal(ent)
		if propose == nil {
			return wb
		}
		resp := &raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{},
		}
		for _, req := range requests {
			switch req.CmdType {
			case raft_cmdpb.CmdType_Put:
				r := &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Put,
					Put:     &raft_cmdpb.PutResponse{},
				}
				resp.Responses = append(resp.Responses, r)
			case raft_cmdpb.CmdType_Delete:
				r := &raft_cmdpb.Response{

					CmdType: raft_cmdpb.CmdType_Delete,
					Delete:  &raft_cmdpb.DeleteResponse{},
				}
				resp.Responses = append(resp.Responses, r)
			case raft_cmdpb.CmdType_Get:
				// 如果是get，为了确保能读到最新数据，应该将前面的batch 写入storage
				d.peerStorage.applyState.AppliedIndex = ent.Index
				wb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
				wb.WriteToDB(d.peerStorage.Engines.Kv)
				wb = &engine_util.WriteBatch{}
				//fmt.Println("debug[134行]: ", d.ctx.engine.Kv == d.peerStorage.Engines.Kv)
				val, err := engine_util.GetCF(d.ctx.engine.Kv, req.Get.Cf, req.Get.Key)
				if err != nil {
					panic(err)
				}
				r := &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Get,
					Get: &raft_cmdpb.GetResponse{
						Value: val,
					},
				}
				resp.Responses = append(resp.Responses, r)
			case raft_cmdpb.CmdType_Snap:
				if msg.Header.RegionEpoch.Version != d.Region().RegionEpoch.Version {
					propose.cb.Done(ErrResp(&util.ErrEpochNotMatch{}))
					return wb
				}
				raft.SplitPrint("[snap] -------")
				d.peerStorage.applyState.AppliedIndex = ent.Index
				wb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
				wb.WriteToDB(d.peerStorage.Engines.Kv)
				wb = &engine_util.WriteBatch{}
				resp.Responses = []*raft_cmdpb.Response{{CmdType: raft_cmdpb.CmdType_Snap,
					Snap: &raft_cmdpb.SnapResponse{Region: d.Region()}}}
				propose.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
			}
			//}
		}
		propose.cb.Done(resp)
	}
	return wb
}

func getRequestKey(req *raft_cmdpb.Request) []byte {
	switch req.CmdType {
	case raft_cmdpb.CmdType_Get:
		return req.Get.Key
	case raft_cmdpb.CmdType_Put:
		return req.Put.Key
	case raft_cmdpb.CmdType_Delete:
		return req.Delete.Key
	}
	return nil
}

func (d *peerMsgHandler) findProposal(ent *eraftpb.Entry) *proposal {
	var proposal *proposal
	if len(d.proposals) > 0 {
		p := d.proposals[0]
		for p.index < ent.Index {
			// 通知那些已经过时的propose结束
			/*
				实验指导书：
				ErrStaleCommand：可能由于领导者的变化，一些日志没有被提交，就被新的领导者的日志所覆盖。
				但是客户端并不知道，仍然在等待响应。所以你应该返回这个命令，让客户端知道并再次重试该命令。
			*/
			p.cb.Done(ErrResp(&util.ErrStaleCommand{}))
			d.proposals = d.proposals[1:]
			if len(d.proposals) == 0 {
				return nil
			}
			p = d.proposals[0]
		}
		if p.index != ent.Index {
			return nil
		}
		if p.term != ent.Term {
			NotifyStaleReq(ent.Term, p.cb)
			d.proposals = d.proposals[1:]
			return nil
		}
		proposal = p
		d.proposals = d.proposals[1:]
	}
	return proposal
	//var propose *proposal
	//if len(d.proposals) > 0 {
	//	p := d.proposals[0]
	//	if p.index == ent.Index {
	//		if p.term != ent.Term {
	//			NotifyStaleReq(ent.Term, p.cb)
	//		} else {
	//			propose = p
	//		}
	//	}
	//	// handle one proposal
	//	d.proposals = d.proposals[1:]
	//}
	//return propose
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
	// 将每条msg封装成一个ent
	//fmt.Println(msg.Requests)
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
	// 不是snap和conf change 这种admin信息
	if msg.AdminRequest == nil {
		// 将指令封装成entries，propose给raft中其他所有的节点
		// 并且在peer的proposals数组中append一个新propose，等待之后callback
		reqMsg, err := msg.Marshal()
		if err != nil {
			log.Fatal("marshal error:", err)
		}
		proposal := &proposal{
			index: d.nextProposalIndex(),
			term:  d.Term(),
			cb:    cb,
		}
		d.proposals = append(d.proposals, proposal)
		//fmt.Println(msg.Requests, "propose", len(d.proposals))
		d.RaftGroup.Propose(reqMsg)
	} else {
		//kvWb := &engine_util.WriteBatch{}
		switch msg.AdminRequest.CmdType {
		case raft_cmdpb.AdminCmdType_CompactLog:
			// 跟上面逻辑保持一致，直接交给 ready 去处理
			// 这里也能直接处理我感觉，但是也不一定会更快，毕竟都是同步的，
			// 而且感觉交给 ready 然后一起 write batch 性能会更好
			data, err := msg.Marshal()
			if err != nil {
				panic(err)
			}
			d.RaftGroup.Propose(data)
		case raft_cmdpb.AdminCmdType_TransferLeader:
			// 作为一个 Raft 命令，TransferLeader 将被提议为一个Raft 日志项。但是 TransferLeader 实际上是一个动作，
			// 不需要复制到其他 peer，所以只需要调用 RawNode 的 TransferLeader() 方法，
			// 而不是 TransferLeader 命令的Propose()。
			d.RaftGroup.TransferLeader(msg.AdminRequest.TransferLeader.Peer.Id)
			cb.Done(&raft_cmdpb.RaftCmdResponse{
				Header: &raft_cmdpb.RaftResponseHeader{},
				AdminResponse: &raft_cmdpb.AdminResponse{
					CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
					TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
				},
			})
		case raft_cmdpb.AdminCmdType_ChangePeer:
			if d.RaftGroup.Raft.PendingConfIndex > d.peerStorage.AppliedIndex() {
				return
			}
			changePeer := msg.AdminRequest.ChangePeer
			//d.proposals = append(d.proposals, &proposal{
			//	cb:    cb,
			//	index: d.nextProposalIndex(),
			//	term:  d.Term(),
			//})
			ctx, _ := msg.Marshal()
			d.RaftGroup.ProposeConfChange(eraftpb.ConfChange{
				ChangeType: changePeer.ChangeType,
				NodeId:     changePeer.Peer.Id,
				Context:    ctx,
			})
		case raft_cmdpb.AdminCmdType_Split:
			split := msg.AdminRequest.Split
			raft.SplitPrint("[propose split] %v", split)
			err := util.CheckKeyInRegion(split.SplitKey, d.Region())
			if err != nil {
				cb.Done(ErrResp(err))
				return
			}
			data, _ := msg.Marshal()
			d.proposals = append(d.proposals, &proposal{
				cb:    cb,
				index: d.nextProposalIndex(),
				term:  d.Term(),
			})
			d.RaftGroup.Propose(data)
		}
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
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
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
