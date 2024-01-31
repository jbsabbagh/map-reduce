package mr

import (
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	workers []worker
	tasks   map[string][]Task
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	c.registerTypes()
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) registerTypes() {
	gob.Register(Task{})
	gob.Register(MapArgs{})
	gob.Register(ReduceArgs{})
	gob.Register(IntermediateFile{})
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	for _, taskType := range c.tasks {
		for _, task := range taskType {
			if !task.IsSuccess() {
				return false
			}
		}
	}
	return true
}

func (c *Coordinator) mapTasksAreDone() bool {
	for _, task := range c.tasks["map"] {
		if !task.IsSuccess() {
			return false
		}
	}
	return true
}

// creates a Coordinator and initializes all the Map & Reduce Tasks.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		tasks:   make(map[string][]Task),
		workers: []worker{},
	}

	intermediateFiles := make(map[int]IntermediateFile)
	c.tasks["map"] = []Task{}
	c.tasks["reduce"] = []Task{}

	for index, file := range files {
		intermediateFileName := fmt.Sprintf("intermediate-%d", index)
		intermediateFilepath := fmt.Sprintf("%s/%s", INTERMEDIATE_DIR, intermediateFileName)
		intermediateFile, _ := os.Create(intermediateFilepath)
		intermediateFiles[index] = IntermediateFile{File: intermediateFile, Mutex: &sync.Mutex{}}

		mapTask := Task{Id: index, Phase: MapPhase, Status: NotStarted, Args: MapArgs{InputFile: file, IntermediateFiles: intermediateFiles, Index: index}}
		c.tasks["map"] = append(c.tasks["map"], mapTask)
	}

	outputFiles := make(map[int]*os.File)
	for index := 0; index < nReduce; index++ {
		oname := fmt.Sprintf("out-%d", index)
		filepath := fmt.Sprintf("%s/%s", OUTPUT_DIR, oname)
		file, _ := os.Create(filepath)
		outputFiles[index] = file

		reduceTask := Task{Id: index, Phase: ReducePhase, Status: NotStarted, Args: ReduceArgs{Index: index, IntermediateDir: INTERMEDIATE_DIR, OutputDir: OUTPUT_DIR, IntermediateFile: fmt.Sprintf("intermediate-%d", index), OutputFileName: oname}}

		c.tasks["reduce"] = append(c.tasks["reduce"], reduceTask)

	}

	c.server()
	return &c
}

func (c *Coordinator) Run() {
	completedTasks := 0
	totalTasks := len(c.tasks["map"]) + len(c.tasks["reduce"])

	for _, task := range c.tasks["map"] {
		if task.IsSuccess() {
			completedTasks++
		}
	}

	for _, task := range c.tasks["reduce"] {
		if task.IsSuccess() {
			completedTasks++
		}
	}
	fmt.Println("Coordinator status:")
	for _, worker := range c.workers {
		fmt.Println(worker)
		fmt.Printf("Completed tasks %d\n", completedTasks)
		fmt.Printf("Total tasks %d\n", totalTasks)
	}
}

func (c *Coordinator) GetTaskStatus(args *TaskStatusArgs, reply *TaskStatusReply) error {
	id := args.Id
	phase := args.Phase
	reply.Ok = true
	switch phase {
	case MapPhase:
		{
			for _, task := range c.tasks["map"] {
				if task.Id == id {
					task.Status = args.Status
				}
			}
		}
	case ReducePhase:
		{
			for _, task := range c.tasks["reduce"] {
				if task.Id == id {
					task.Status = args.Status
				}
			}
		}
	}
	return nil
}

func (c *Coordinator) SendTask(args *NewTaskArgs, reply *NewTaskReply) error {
	fmt.Println("Task requested")
	if !c.mapTasksAreDone() {
		fmt.Println("Map tasks are not done - Fetching map task")
		for _, task := range c.tasks["map"] {
			if task.Status == NotStarted {
				fmt.Printf("Map task found %d\n", task.Id)
				reply.Id = task.Id
				reply.Phase = task.Phase
				reply.Status = task.Status
				reply.Args = task.Args
				return nil
			}
		}
	} else {
		fmt.Println("Map tasks are done - Fetching reduce task")
		for _, task := range c.tasks["reduce"] {
			fmt.Printf("Reduce task found %d", task.Id)
			if task.Status == NotStarted {
				fmt.Printf("Reduce task found %d", task.Id)
				reply.Id = task.Id
				reply.Phase = task.Phase
				reply.Status = task.Status
				reply.Args = task.Args
				return nil
			}
		}
	}
	return nil
}

func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	workerId := args.WorkerId
	c.workers = append(c.workers, worker{workerId, "idle"})
	reply.Ok = true

	return nil
}

type worker struct {
	id     int
	status string
}
