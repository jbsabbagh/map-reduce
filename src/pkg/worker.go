package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Helper function to make a worker and trigger set up
func MakeWorker(buckets int) Worker {
	worker := Worker{
		Id:        os.Getpid(),
		Status:    Idle,
		Buckets:   buckets,
		WorkerDir: fmt.Sprintf("/tmp/%d", os.Getpid()),
		Logger:    log.New(os.Stdout, fmt.Sprintf("Worker %d: ", os.Getpid()), log.Lshortfile|log.Ltime|log.Ldate),
	}
	worker.Logger.Print("Setting up Worker", worker)
	worker.setUp()
	return worker
}

type WorkerStatus int

const (
	Idle        WorkerStatus = 0
	RunningTask WorkerStatus = 1
	Dead        WorkerStatus = 2
)

type Worker struct {
	Id           int
	Status       WorkerStatus
	AssignedTask Task
	Buckets      int
	WorkerDir    string
	Logger       *log.Logger
}

func (w *Worker) AssignTask(task Task) bool {
	w.AssignedTask = task
	w.Status = RunningTask

	return true
}

func (w Worker) Run(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for true {
		task, err := w.getTask()

		if err != nil {
			w.Logger.Println("No Tasks Found - Sleeping")
			time.Sleep(time.Second)
			continue
		}

		switch task.GetTaskType() {
		case Map:
			{
				task := task.(MapTask)
				task.Status = Running
				w.Logger.Printf("Received map task ID %d\n", task.Id)

				intermediateDir := fmt.Sprintf("%s/%d", w.WorkerDir, task.Id)

				err := os.Mkdir(intermediateDir, 0755)
				if err != nil {
					w.Logger.Printf("Error creating intermediate directory for Task ID %d", task.Id)
					w.Logger.Fatal(err)
				}

				files := CreateOutputFiles(BUCKETS, intermediateDir)
				content := ReadFile(task.InputFile)
				keyValues := mapf(task.InputFile, content)
				for _, kv := range keyValues {
					bucket := ihash(kv.Key) % BUCKETS
					file := files[bucket]
					file.Write(kv.Key, kv.Value)

				}
				w.Logger.Printf("Map task ID %d completed\n", task.Id)
				task.SetStatus(Success)
				w.sendTaskCompletion(task)
			}
		case Reduce:
			{
				task := task.(ReduceTask)
				task.Status = Running
				w.Logger.Println("Received reduce task\n", task)
				outputPath := filepath.Join(task.OutputDir, task.OutputFileName)
				outputFile, err := os.Create(outputPath)

				if err != nil {
					w.Logger.Fatal("Error creating output file", err)
				} else {
					w.Logger.Printf("Output file created %s", outputPath)
				}

				defer outputFile.Close()
				intermediate := w.loadDataFromIntermediateFiles(task.WorkerDirs, task.Index)
				sort.Sort(ByKey(intermediate))

				i := 0
				for i < len(intermediate) {
					j := i + 1
					for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, intermediate[k].Value)
					}
					output := reducef(intermediate[i].Key, values)

					WriteRecord(outputFile, intermediate[i].Key, output)

					i = j
				}
				w.Logger.Printf("Reduce task ID %d completed\n", task.Id)
				task.SetStatus(Success)
				w.sendTaskCompletion(task)

			}
		}
	}

}

func (w Worker) loadDataFromIntermediateFiles(workerDirs []string, index int) []KeyValue {
	intermediate := []KeyValue{}
	for _, workerDir := range workerDirs {
		dirs, _ := os.ReadDir(workerDir)

		for _, dir := range dirs {

			fileName := fmt.Sprintf("%s/%s/intermediate-%d", workerDir, dir.Name(), index)
			w.Logger.Printf("Loading data from file %s", fileName)
			keyValues := LoadDataFromIntermediateFile(fileName)
			intermediate = append(intermediate, keyValues...)
		}
	}
	return intermediate
}

func (w Worker) Hearbeat() {

	heartbeat := time.Second * 2
	go func(heartbeat time.Duration) {
		for true {
			w.Logger.Println("Sending heartbeat")
			args := HeartbeatArgs{
				WorkerId:  w.Id,
				Heartbeat: time.Now(),
			}
			reply := HeartbeatReply{}
			w.call("Coordinator.Heartbeat", &args, &reply)
			time.Sleep(heartbeat)
		}
	}(heartbeat)
}

func (w Worker) getTask() (Task, error) {
	args := NewTaskArgs{WorkerId: w.Id}
	reply := NewTaskReply{}
	w.call("Coordinator.SendTask", &args, &reply)
	if !reply.Ok {
		return nil, fmt.Errorf("Failed to get task")
	}

	switch reply.Type {
	case Map:
		args := reply.Args
		id, _ := strconv.Atoi(args["Id"])
		index, _ := strconv.Atoi(args["Index"])
		task := MapTask{
			Id:        id,
			Status:    NotStarted,
			InputFile: args["InputFile"],
			Index:     index,
			Type:      Map,
		}
		return task, nil
	case Reduce:
		args := reply.Args
		id, _ := strconv.Atoi(args["Id"])
		index, _ := strconv.Atoi(args["Index"])
		task := ReduceTask{
			Id:             id,
			Status:         NotStarted,
			Index:          index,
			OutputDir:      args["OutputDir"],
			OutputFileName: args["OutputFileName"],
			WorkerDirs:     reply.WorkerDirs,
			Type:           Reduce,
		}
		return task, nil
	}
	return nil, fmt.Errorf("Unknown task type")
}

func (w Worker) setUp() {

	w.Logger.Printf("Creating worker directory %s", w.WorkerDir)
	err := os.Mkdir(w.WorkerDir, 0755)
	if err != nil {
		w.Logger.Fatal("Error creating worker directory", err)
	}

	w.registerWorker()
}

func (w Worker) registerWorker() {
	workerId := w.Id
	args := RegisterWorkerArgs{
		Id:        workerId,
		Status:    Idle,
		Buckets:   w.Buckets,
		WorkerDir: w.WorkerDir,
		Heatbeat:  time.Now(),
	}
	reply := RegisterWorkerReply{}
	w.call("Coordinator.RegisterWorker", &args, &reply)

	if reply.Ok {
		w.Logger.Printf("Worker %d registered successfully\n", workerId)
	} else {
		w.Logger.Printf("Worker %d registration failed\n", workerId)
	}
}

func (w Worker) sendTaskCompletion(task Task) {
	args := TaskStatusArgs{
		Id:     task.GetId(),
		Status: task.GetStatus(),
		Type:   task.GetTaskType(),
	}
	reply := TaskStatusReply{}
	w.call("Coordinator.GetTaskStatus", &args, &reply)

	if !reply.Ok {
		w.Logger.Println("Notification to the Coordinator failed")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func (w Worker) call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		w.Logger.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	w.Logger.Println(err)
	return false
}
