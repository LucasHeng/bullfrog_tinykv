package raft

import (
	"log"

	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
)

// Debugging
const Debug = true
const flag = "2B"

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func To2B(format string, a ...interface{}) (n int, err error) {
	if Debug && flag == "2B" {
		log.Printf(format, a...)
	}
	return
}

// func PrintMessage(m pb.Message) {
// 	msgs := make([]Printmsg, 0)
// 	if len(m.Entries) != 0 {
// 		for _, e := range m.Entries {
// 			cmd := &raft_cmdpb.RaftCmdRequest{}
// 			cmd.Unmarshal(e.Data)
// 			msgs = append(msgs, Printmsg{Term: e.Term, Index: e.Index, Msg: cmd})
// 		}
// 	}
// 	To2B("{Node %d} recieve from Node:%d {msg:%v Term: %d; logTerm: %d Index:%d Entries:%v} in {term : %d} with {state: %v}", r.id, m.From, m.MsgType, m.Term, m.LogTerm, m.Index, msgs, r.Term, r.State.String())
//
// }

const pready = true

func PrintReady(rd Ready, id uint64) {
	msgs := make([]Printmsg, 0)
	if len(rd.CommittedEntries) != 0 {
		for _, e := range rd.CommittedEntries {
			cmd := &raft_cmdpb.RaftCmdRequest{}
			cmd.Unmarshal(e.Data)
			msgs = append(msgs, Printmsg{Term: e.Term, Index: e.Index, Msg: cmd})
		}
		if pready {
			DPrintf("Node:%d Ready recieve ss:%v hs:%v ents:%v", id, rd.SoftState, rd.HardState, msgs)
		}
	}
}
