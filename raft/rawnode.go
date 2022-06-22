// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// ErrStepLocalMsg is returned when try to step a local raft message
var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

// ErrStepPeerNotFound is returned when try to step a response message
// but there is no peer found in raft.Prs for that node.
var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

// SoftState provides state that is volatile and does not need to be persisted to the WAL.
type SoftState struct {
	Lead      uint64
	RaftState StateType
}

// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	*SoftState

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.
	pb.HardState

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	Entries []pb.Entry

	// Snapshot specifies the snapshot to be saved to stable storage.
	Snapshot pb.Snapshot

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	CommittedEntries []pb.Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MessageType_MsgSnapshot message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	Messages []pb.Message
}

// RawNode is a wrapper of Raft.
type RawNode struct {
	Raft *Raft

	prevHs pb.HardState
	prevSS SoftState

	commitSinceIndex uint64
	// Your Data Here (2A).
}

// NewRawNode returns a new RawNode given configuration and a list of raft peers.
func NewRawNode(config *Config) (*RawNode, error) {
	// Your Code Here (2A).
	rn := &RawNode{Raft: newRaft(config)}
	rn.prevHs = rn.Raft.hardState()
	rn.prevSS = rn.Raft.softState()
	rn.commitSinceIndex = config.Applied
	return rn, nil
}

// Tick advances the internal logical clock by a single tick.
func (rn *RawNode) Tick() {
	rn.Raft.tick()
}

// Campaign causes this RawNode to transition to candidate state.
func (rn *RawNode) Campaign() error {
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgHup,
	})
}

// Propose proposes data be appended to the raft log.
func (rn *RawNode) Propose(data []byte) error {
	ent := pb.Entry{Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    rn.Raft.id,
		Entries: []*pb.Entry{&ent}})
}

// ProposeConfChange proposes a config change.
func (rn *RawNode) ProposeConfChange(cc pb.ConfChange) error {
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	ent := pb.Entry{EntryType: pb.EntryType_EntryConfChange, Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{&ent},
	})
}

// ApplyConfChange applies a config change to the local node.
func (rn *RawNode) ApplyConfChange(cc pb.ConfChange) *pb.ConfState {
	if cc.NodeId == None {
		return &pb.ConfState{Nodes: nodes(rn.Raft)}
	}
	switch cc.ChangeType {
	case pb.ConfChangeType_AddNode:
		rn.Raft.addNode(cc.NodeId)
	case pb.ConfChangeType_RemoveNode:
		rn.Raft.removeNode(cc.NodeId)
	default:
		panic("unexpected conf type")
	}
	return &pb.ConfState{Nodes: nodes(rn.Raft)}
}

// Step advances the state machine using the given message.
func (rn *RawNode) Step(m pb.Message) error {
	// ignore unexpected local messages receiving over network
	if IsLocalMsg(m.MsgType) {
		return ErrStepLocalMsg
	}
	if pr := rn.Raft.Prs[m.From]; pr != nil || !IsResponseMsg(m.MsgType) {
		return rn.Raft.Step(m)
	}
	return ErrStepPeerNotFound
}

// Ready returns the current point-in-time state of this RawNode.
func (rn *RawNode) Ready() Ready {
	// Your Code Here (2A).
	rd := Ready{}
	// ss是否有更新
	ss := rn.Raft.softState()
	if !compareSS(ss, rn.prevSS) {
		rd.SoftState = &ss
	}
	// hs是否有更新,hs也不能为空
	hs := rn.Raft.hardState()
	if !isEmtpyHardState(hs) && !compareHs(hs, rn.prevHs) {
		rd.HardState = hs
	}
	// 是否有还未stable的entry
	if len(rn.Raft.RaftLog.unstableEntries()) != 0 {
		// 找到未stabled的entries
		rd.Entries = rn.Raft.RaftLog.unstableEntries()
		if flag == "copy" || flag == "all" {
			DPrintf("entries: %v", rd.Entries)
		}
	}
	// 是否还有commit但是还未apply的entries
	if len(rn.Raft.RaftLog.nextEnts()) > 0 {
		rd.CommittedEntries = rn.Raft.RaftLog.nextEnts()
		if flag == "copy" || flag == "all" {
			DPrintf("committedEntries: %v", rd.CommittedEntries)
		}
	}
	PrintReady(rd, rn.Raft.id)
	// 是否有新的消息
	if len(rn.Raft.msgs) != 0 {
		rd.Messages = rn.Raft.msgs
	}
	rn.Raft.msgs = make([]pb.Message, 0)
	return rd
}

// hardstate比较
func compareHs(l pb.HardState, r pb.HardState) bool {
	return l.Term == r.Term && l.Vote == r.Vote && l.Commit == r.Commit
}

func isEmtpyHardState(r pb.HardState) bool {
	return compareHs(r, pb.HardState{})
}

// HasReady called when RawNode user need to check if any Ready pending.
func (rn *RawNode) HasReady() bool {
	// Your Code Here (2A).
	// 判断是否有新的东西需要更新
	// 如新的entries，新的应用entries
	// 新的状态？

	// 有新的消息
	if len(rn.Raft.msgs) != 0 {
		return true
	}

	// 状态更新
	if rn.Raft.softState() != rn.prevSS {
		return true
	}
	if !isEmtpyHardState(rn.Raft.hardState()) && !compareHs(rn.Raft.hardState(), rn.prevHs) {
		return true
	}
	// 有未持久的entries
	if rn.Raft.RaftLog.LastIndex() > rn.Raft.RaftLog.stabled {
		return true
	}
	// 有未应用的commit entry
	if len(rn.Raft.RaftLog.nextEnts()) > 0 {
		return true
	}
	return false
}

// Advance notifies the RawNode that the application has applied and saved progress in the
// last Ready results.
func (rn *RawNode) Advance(rd Ready) {
	// Your Code Here (2A).
	// hs有更新就换
	if !compareHs(rd.HardState, pb.HardState{}) {
		rn.prevHs = rd.HardState
	}
	// ss有更新就换
	if rd.SoftState != nil {
		if !compareSS(*rd.SoftState, SoftState{}) {
			rn.prevSS = *rd.SoftState
		}
	}
	// 将entries持久化，更新stabled
	if len(rd.Entries) != 0 {
		e := rd.Entries[len(rd.Entries)-1]
		rn.Raft.RaftLog.stableTo(e.Index, e.Term)
	}
	// 更新已经commit且交给上层应用了的log index
	if len(rd.CommittedEntries) != 0 {
		rn.commitSinceIndex = rd.CommittedEntries[len(rd.CommittedEntries)-1].Index
		rn.Raft.RaftLog.applied = rn.commitSinceIndex
	}
}

// GetProgress return the Progress of this node and its peers, if this
// node is leader.
func (rn *RawNode) GetProgress() map[uint64]Progress {
	prs := make(map[uint64]Progress)
	if rn.Raft.State == StateLeader {
		for id, p := range rn.Raft.Prs {
			prs[id] = *p
		}
	}
	return prs
}

// TransferLeader tries to transfer leadership to the given transferee.
func (rn *RawNode) TransferLeader(transferee uint64) {
	_ = rn.Raft.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: transferee})
}

func compareSS(s SoftState, ns SoftState) bool {
	return s.Lead == ns.Lead && s.RaftState == ns.RaftState
}
