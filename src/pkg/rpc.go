package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

type TaskStatusArgs struct {
	Id     int
	Status TaskStatus
	Type   TaskType
}

type TaskStatusReply struct {
	Ok bool
}

type NewTaskArgs struct {
	WorkerId int
}

type NewTaskReply struct {
	Ok         bool
	Type       TaskType
	Args       map[string]string
	WorkerDirs []string
}

type RegisterWorkerArgs struct {
	Id        int
	Status    WorkerStatus
	Buckets   int
	WorkerDir string
	Heatbeat  time.Time
}

type RegisterWorkerReply struct {
	Ok bool
}

type HeartbeatArgs struct {
	WorkerId  int
	Heartbeat time.Time
}

type HeartbeatReply struct {
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
