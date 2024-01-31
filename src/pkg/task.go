package mr

type Task struct {
	Id     int
	Phase  TaskPhase
	Status TaskStatus
	Args   Args
}

type MapTask struct {
	Id                int
	Status            TaskStatus
	InputFile         string
	IntermediateFiles map[int]IntermediateFile
	Index             int
}

type ReduceTask struct {
	Id               int
	Status           TaskStatus
	Index            int
	IntermediateDir  string
	OutputDir        string
	IntermediateFile string
	OutputFileName   string
}

func (t *Task) IsSuccess() bool {
	return t.Status == Success
}

type TaskPhase int

const (
	MapPhase    TaskPhase = 0
	ReducePhase TaskPhase = 1
)

type TaskStatus int

const (
	NotStarted TaskStatus = 0
	Running    TaskStatus = 1
	Success    TaskStatus = 2
	Failure    TaskStatus = 3
)

type Args interface{}

type MapArgs struct {
	InputFile         string
	IntermediateFiles map[int]IntermediateFile
	Index             int
}

type ReduceArgs struct {
	Index            int
	IntermediateDir  string
	OutputDir        string
	IntermediateFile string
	OutputFileName   string
}
