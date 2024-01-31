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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type TaskStatusArgs struct {
	Id     int
	Status TaskStatus
	Phase  TaskPhase
}

type TaskStatusReply struct {
	Ok bool
}

type NewTaskArgs struct {
}

type NewTaskReply struct {
	Id     int
	Status TaskStatus
	Phase  TaskPhase
	Args   Args
}

type RegisterWorkerArgs struct {
	WorkerId int
}

type RegisterWorkerReply struct {
	Ok bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
