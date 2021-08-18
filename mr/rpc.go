package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type RequestTaskArgs struct {
	WorkerId int
}

type RequestTaskReply struct {
	Input   []string
	ReduceN int
	Index   int
	Type    TaskType
}

type ResponseTaskArgs struct {
	Index   int
	Buckets []int
	Type    TaskType
}

type ResponseTaskReply struct{}

// Add your RPC definitions here and here

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func containsInt(value int, arr []int) bool {
	for _, i := range arr {
		if value == i {
			return true
		}
	}
	return false
}
