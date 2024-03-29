package mr

type Task interface {
	IsSuccess() bool
	GetTaskType() TaskType
	GetId() int
	GetStatus() TaskStatus
	GetStatusStr() string
}
type MapTask struct {
	Id        int
	Status    TaskStatus
	InputFile string
	Index     int
	Type      TaskType
}

func (t MapTask) IsSuccess() bool {
	return t.Status == Success
}

func (t MapTask) GetId() int {
	return t.Id
}

func (t MapTask) GetStatus() TaskStatus {
	return t.Status
}

func (t MapTask) GetStatusStr() string {
	switch t.Status {
	case NotStarted:
		return "Not Started"
	case Running:
		return "Running"
	case Success:
		return "Success"
	case Failure:
		return "Failure"
	default:
		return "Unknown"
	}
}

func (t MapTask) GetTaskType() TaskType {
	return t.Type
}

func (t *MapTask) SetStatus(status TaskStatus) {
	t.Status = status
}

type ReduceTask struct {
	Id             int
	Status         TaskStatus
	Index          int
	OutputDir      string
	OutputFileName string
	WorkerDirs     []string
	Type           TaskType
}

func (t ReduceTask) IsSuccess() bool {
	return t.Status == Success
}

func (t ReduceTask) GetId() int {
	return t.Id
}

func (t ReduceTask) GetStatus() TaskStatus {
	return t.Status
}

func (t ReduceTask) GetStatusStr() string {
	switch t.Status {
	case NotStarted:
		return "Not Started"
	case Running:
		return "Running"
	case Success:
		return "Success"
	case Failure:
		return "Failure"
	default:
		return "Unknown"
	}
}

func (t *ReduceTask) SetStatus(status TaskStatus) {
	t.Status = status
}

func (t ReduceTask) GetTaskType() TaskType {
	return t.Type
}

type TaskType int

const (
	Map    TaskType = 0
	Reduce TaskType = 1
)

type TaskStatus int

const (
	NotStarted TaskStatus = 0
	Running    TaskStatus = 1
	Success    TaskStatus = 2
	Failure    TaskStatus = 3
)
