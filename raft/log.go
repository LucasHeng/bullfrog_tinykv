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
	"fmt"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	id uint64 //
	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		// 如果没有已提交日志，那么commitindex初始化为0
		firstIndex = 1
	}
	lastindex, err := storage.LastIndex()
	if err != nil {
		lastindex = 0
	}
	entries, _ := storage.Entries(firstIndex, lastindex+1)
	return &RaftLog{
		storage:         storage,
		committed:       firstIndex - 1,
		applied:         firstIndex - 1,
		stabled:         lastindex,
		entries:         entries,
		pendingSnapshot: nil, // not used in 2A
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	// 若stabled在中
	// 截掉
	if len(l.entries) != 0 && l.stabled >= l.entries[0].Index {
		l.entries = l.entries[l.stabled+1-l.entries[0].Index:]
	}
	return l.entries
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	ents, _ = l.findentries(l.applied+1, l.committed+1)
	return ents
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if i, ok := l.unstableLastIndex(); ok {
		return i
	}
	lastindex, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	return lastindex
}

// 最后的entry的term
func (l *RaftLog) LastTerm() uint64 {
	term, _ := l.Term(l.LastIndex())
	return term
}

func (l *RaftLog) isUpToDate(index uint64, term uint64) bool {
	return term > l.LastTerm() || (term == l.LastTerm() && index >= l.LastIndex())
}

func (l *RaftLog) unstableLastIndex() (uint64, bool) {
	if len(l.entries) != 0 {
		return l.entries[len(l.entries)-1].Index, true
	}
	if l.pendingSnapshot != nil {
		return l.pendingSnapshot.Metadata.Index, true
	}
	return 0, false
}

func (l *RaftLog) FirstIndex() uint64 {
	if l.pendingSnapshot != nil {
		return l.pendingSnapshot.Metadata.Index + 1
	}
	index, err := l.storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	return index
}

func (l *RaftLog) unstableTerm(i uint64) (uint64, bool) {
	if i <= l.stabled {
		if l.pendingSnapshot != nil && l.pendingSnapshot.Metadata.Index == i {
			return l.pendingSnapshot.Metadata.Term, true
		}
		return 0, false
	}

	last, ok := l.unstableLastIndex()
	if !ok {
		return 0, false
	}
	if i > last {
		return 0, false
	}

	return l.entries[i-l.stabled-1].Term, true
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	// 有未persist的snapshot
	// lastindex := l.LastIndex()
	// if i > lastindex {
	// 	return 0, fmt.Errorf("index out of range")
	// }
	// if i > l.stabled {
	// 	return l.entries[i-l.entries[0].Index].Term, nil
	// }
	// return l.storage.Term(i)
	dummyindex := l.FirstIndex() - 1
	lastindex := l.LastIndex()
	// log.Infof("dummyindex:%d lastindex:%d i:%d stable:%d", dummyindex, lastindex, i, l.stabled)
	if i < dummyindex || i > lastindex {
		log.Infof("index out of range")
		return 0, nil
	}

	if t, ok := l.unstableTerm(i); ok {
		return t, nil
	}

	// last, ok := l.unstableLastIndex()
	// if l.stabled < i && i <= last && ok {
	// 	return l.entries[i-l.stabled-1].Term, nil
	// }
	//
	// if i <= l.stabled {
	// 	// 刚好是新来的snap
	// 	if l.pendingSnapshot != nil && l.pendingSnapshot.Metadata.Index == i {
	// 		return l.pendingSnapshot.Metadata.Term, nil
	// 	}
	// }

	term, err := l.storage.Term(i)
	if err != nil {
		if err == ErrCompacted || err == ErrUnavailable {
			return 0, err
		}
		log.Infof("Node:%d dummyindex:%d lastindex:%d i:%d stable:%d", l.id, dummyindex, lastindex, i, l.stabled)
		panic(err)
	}
	return term, nil
}

func (l *RaftLog) appliedTo(i uint64, id uint64) {
	if i == 0 {
		return
	}
	if l.committed < i || i < l.applied {
		log.Fatal(fmt.Sprintf("Node:%d applied(%d) is out of range [prevApplied(%d), committed(%d)]", id, i, l.applied, l.committed))
	}
	l.applied = i
}

func (l *RaftLog) checkoutofbound(lo, hi uint64) error {
	// 区间判定
	if lo > hi {
		log.Panicf("invalid range:lo %d and hi %d", lo, hi)
	}

	if lo < l.FirstIndex() {
		return ErrCompacted
	}

	if hi > l.LastIndex()+1 {
		log.Panicf("illegal slice bound[%d,%d) out of bound[%d,%d]", lo, hi, l.FirstIndex(), l.LastIndex())
	}
	return nil
}

// 获取在非stable的entries
func (l *RaftLog) findUnstableentries(lo, hi uint64) []pb.Entry {
	if lo > hi {
		log.Panicf("Node:%d invalid unstable slice %d > %d", lo, hi)
	}
	upper := l.stabled + 1 + uint64(len(l.entries))
	if lo <= l.stabled || hi > upper {
		log.Panicf("Node:%d invalid unstable slice [%d,%d) out of bound[%d,%d]", lo, hi, l.stabled+1, upper)
	}
	return l.entries[lo-l.stabled-1 : hi-l.stabled-1]
}

func (l *RaftLog) appentries(i uint64) ([]pb.Entry, error) {
	if i > l.LastIndex() {
		return nil, nil
	}
	return l.findentries(i, l.LastIndex()+1)
}

// 获得相应区间的entries
func (l *RaftLog) findentries(lo uint64, hi uint64) ([]pb.Entry, error) {

	err := l.checkoutofbound(lo, hi)
	if err != nil {
		return nil, err
	}
	if lo == hi {
		return nil, nil
	}

	var ents []pb.Entry
	// 如果有一部分在storage里面，先找那一部分
	if lo <= l.stabled {
		log.Infof("Node:%d lo:%d hi:%d stable:%d", l.id, lo, hi, l.stabled)
		stable_ents, err := l.storage.Entries(lo, min(hi, l.stabled+1))
		if err != nil {
			if err == ErrCompacted {
				return nil, err
			} else if err == ErrUnavailable {
				log.Panicf("entries[%d:%d) is unavailable from storage", lo, min(hi, l.stabled+1))
			}
			log.Panicf("lo:%d hi:%d stable:%d", lo, hi, l.stabled)
		}
		// ents = append(ents, stable_ents...)
		// 比复制更快
		ents = stable_ents
	}
	// 有未unstabled的部分
	if hi > l.stabled+1 {
		// firstindex := l.entries[0].Index
		unstable_ents := l.findUnstableentries(max(lo, l.stabled+1), hi)
		if len(ents) > 0 {
			combined := make([]pb.Entry, len(ents)+len(unstable_ents))
			n := copy(combined, ents)
			copy(combined[n:], unstable_ents)
			ents = combined
		} else {
			ents = unstable_ents
		}
		// ents = append(ents, l.entries[max(l.stabled+1, lo)-firstindex:hi-firstindex]...)
	}
	if flag == "copy" || flag == "all" {
		// DPrintf("log.go line 101 ents:%d", len(ents))
	}
	return ents, nil
}

// 返回snap
func (l *RaftLog) findSnap() (pb.Snapshot, error) {
	if l.pendingSnapshot != nil {
		return *l.pendingSnapshot, nil
	}
	return l.storage.Snapshot()
}

// 判断是否有snap
func (l *RaftLog) hasPendingSnapshot() bool {
	return l.pendingSnapshot != nil && !IsEmptySnap(l.pendingSnapshot)
}

// 加入新的entry
func (l *RaftLog) AppendEntries(ents ...*pb.Entry) {
	if len(ents) == 0 {
		return
	}
	start := ents[0].Index
	l.stabled = min(l.stabled, start-1)
	// 如果当前的RaftLog.entries是空，或者非空但是start是刚好是下一个
	// 非空的话，和第一个比较
	if len(l.entries) == 0 {
		// 空的话什么都不做
	} else if start <= l.entries[0].Index {
		// 加入的ents在unstable entries之前，则前面的要推导重来
		l.entries = []pb.Entry{}
	} else if start > l.entries[0].Index {
		// 截掉ents之后的部分
		l.entries = l.entries[0 : start-l.entries[0].Index]
	}
	for _, ent := range ents {
		l.entries = append(l.entries, *ent)
	}
}

func (l *RaftLog) commitTo(commit uint64) {
	if l.committed < commit {
		if commit > l.LastIndex() {
			log.Fatalf("Node:%d To commit log index:%d > LastIndex:%d", l.id, commit, l.LastIndex())
		}
		l.committed = commit
	}
}

func (l *RaftLog) stableTo(stable, term uint64) {
	st, err := l.Term(stable)
	if err != nil {
		// 出错应该是，这一块log已经删掉了，已经应用了
		return
	}

	if st == term && stable > l.stabled {
		l.entries = l.entries[stable-l.stabled:]
		l.stabled = stable
		// 是否收缩entry
		//
	}

}

// 某个index之后，是否还有已经 commit 的 entries
func (l *RaftLog) hasEntriesSince(index uint64) bool {
	firstIndex, _ := l.storage.FirstIndex()
	offset := max(index+1, firstIndex)
	high := l.committed + 1
	if flag == "copy" || flag == "all" {
		DPrintf("Node find entries_since from lo: %d to hi: %d", offset, high)
	}
	return high > offset
}

// 返回某个index后的entries
// func (l *RaftLog) entriesSince(index uint64) []pb.Entry {
// 	firstindex, _ := l.storage.FirstIndex()
// 	offset := max(index+1, firstindex)
// 	high := l.committed + 1
// 	if high > offset {
// 		if flag == "copy" || flag == "all" {
// 			DPrintf("Node find entries_since from lo: %d to hi: %d", offset, high)
// 		}
// 		return l.findentries(offset, high)
// 	}
// 	return []pb.Entry{}
// }

func (l *RaftLog) matchTerm(i, term uint64) bool {
	t, err := l.Term(i)
	if err != nil {
		return false
	}
	return t == term
}

func (l *RaftLog) restore(s *pb.Snapshot) {
	log.Infof("log [%v] starts to restore snapshot [index: %d, term: %d]", l, s.Metadata.Index, s.Metadata.Term)
	// 这里不能用commitTo
	l.committed = s.Metadata.Index
	l.stabled = s.Metadata.Index
	l.entries = []pb.Entry{}
	l.pendingSnapshot = s
}
