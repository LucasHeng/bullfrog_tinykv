package raft

import "log"

// Debugging
const Debug = true
const flag = "all"

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
