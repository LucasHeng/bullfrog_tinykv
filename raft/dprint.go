package raft

import "log"

// Debugging
const Debug = false
const flag = "2B"

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
