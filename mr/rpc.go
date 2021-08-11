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

type RequestWorkArgs struct {
	WorkerId int
}

type RequestWorkReply struct {
	File    []string
	ReduceN int
	Index   int
	Type    TaskType
}

type CompleteWorkArgs struct {
	Type  TaskType
	Index int
}
type CompleteWorkReply struct {
}

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
