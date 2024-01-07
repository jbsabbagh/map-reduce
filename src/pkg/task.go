package mr

type Task struct {
	Id     int
	Phase  TaskPhase
	Status TaskStatus
	Args   Args
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

type Args interface{}

type MapArgs struct {
	Filename string
	Files    map[int]IntermediateFile
}

type ReduceArgs struct {
	Index            int
	IntermediateDir  string
	OutputDir        string
	IntermediateFile string
	OutputFileName   string
}
