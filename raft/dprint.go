package raft

import "log"

// Debugging
const Debug = false
const flag = "Debug2B"

var ToB = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func ToBPrint(format string, a ...interface{}) {
	if flag == "Debug2B" && ToB == true {
		log.Printf(format, a...)
	}
}
