package mr

type Task struct {
	Id     int
	Phase  TaskPhase
	Status TaskStatus
}

type TaskPhase int

const (
	MapPhase    TaskPhase = 0
	ReducePhase TaskPhase = 1
)

type TaskStatus int

const (
	Idle    TaskStatus = 0
	Running TaskStatus = 1
	Success TaskStatus = 2
	Failure TaskStatus = 3
)
