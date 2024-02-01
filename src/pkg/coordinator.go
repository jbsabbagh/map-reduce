package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	Logger      *log.Logger
	workers     []Worker
	mapTasks    []MapTask
	reduceTasks []ReduceTask
	taskMutex   *sync.Mutex
	workerMutex *sync.Mutex
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		c.Logger.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.taskMutex.Lock()
	defer c.taskMutex.Unlock()

	for _, task := range c.mapTasks {
		if !task.IsSuccess() {
			return false
		}
	}

	for _, task := range c.reduceTasks {
		if !task.IsSuccess() {
			return false
		}

	}
	c.Logger.Println("All tasks are done!")
	return true

}

func (c *Coordinator) mapTasksAreDone() bool {
	for _, task := range c.mapTasks {
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
		mapTasks:    make([]MapTask, 0),
		reduceTasks: make([]ReduceTask, 0),
		workers:     []Worker{},
		Logger:      log.New(os.Stdout, "Coordinator: ", log.Lshortfile|log.Ltime|log.Ldate),
		taskMutex:   &sync.Mutex{},
		workerMutex: &sync.Mutex{},
	}

	for index, file := range files {

		mapTask := MapTask{
			Id:        index,
			Status:    NotStarted,
			InputFile: file,
			Index:     index,
		}
		c.mapTasks = append(c.mapTasks, mapTask)
	}

	outputFiles := make(map[int]*os.File)
	for index := 0; index < nReduce; index++ {
		oname := fmt.Sprintf("out-%d", index)
		filepath := fmt.Sprintf("%s/%s", OUTPUT_DIR, oname)
		file, _ := os.Create(filepath)
		outputFiles[index] = file

		reduceTask := ReduceTask{
			Id:               index,
			Status:           NotStarted,
			Index:            index,
			IntermediateDir:  INTERMEDIATE_DIR,
			OutputDir:        OUTPUT_DIR,
			IntermediateFile: fmt.Sprintf("intermediate-%d", index),
			OutputFileName:   oname,
		}

		c.reduceTasks = append(c.reduceTasks, reduceTask)

	}

	c.server()
	return &c
}

// DisplayStatistics prints the current status of the coordinator
func (c *Coordinator) DisplayStatistics() {

	// TODO: This can be inefficient as we are locking the mutex for the entire duration of the function
	// Look into RWMutex + tradeoffs
	c.workerMutex.Lock()
	defer c.workerMutex.Unlock()

	completedTasks := 0
	totalTasks := len(c.mapTasks) + len(c.reduceTasks)

	for _, task := range c.mapTasks {
		if task.IsSuccess() {
			completedTasks++
		}
	}

	for _, task := range c.reduceTasks {
		if task.IsSuccess() {
			completedTasks++
		}
	}
	c.Logger.Println("Coordinator status:")
	for _, worker := range c.workers {
		c.Logger.Println(worker)
		c.Logger.Printf("Completed tasks %d", completedTasks)
		c.Logger.Printf("Total tasks %d", totalTasks)
	}
}

func (c *Coordinator) GetTaskStatus(args *TaskStatusArgs, reply *TaskStatusReply) error {
	c.taskMutex.Lock()
	defer c.taskMutex.Unlock()

	id := args.Id
	taskType := args.Type
	reply.Ok = true

	c.Logger.Printf("Received task status for id %d", id)
	switch taskType {
	case Map:
		{
			for index := range c.mapTasks {
				if c.mapTasks[index].Id == id {
					c.mapTasks[index].SetStatus(args.Status) // Set the status of the task
					c.Logger.Printf("Map task status updated %d to Success!", id)
				}
			}

		}
	case Reduce:
		{
			for index := range c.reduceTasks {
				if c.reduceTasks[index].Id == id {
					c.reduceTasks[index].SetStatus(args.Status) // Set the status of the task
					c.Logger.Printf("Reduce task status updated %d to Success!", id)
				}
			}
		}
	}
	return nil
}

func (c *Coordinator) SendTask(args *NewTaskArgs, reply *NewTaskReply) error {
	c.Logger.Println("Task requested")
	if !c.mapTasksAreDone() {
		c.Logger.Println("Map tasks are not done - Fetching map task")
		for _, task := range c.mapTasks {
			if task.Status == NotStarted {
				fmt.Printf("Map task found %d\n", task.Id)
				reply.Type = Map
				reply.Ok = true
				reply.Args = map[string]string{
					"InputFile": task.InputFile,
					"Index":     fmt.Sprintf("%d", task.Index),
					"Id":        fmt.Sprintf("%d", task.Id),
				}

				return nil
			}
		}
	} else {
		reply.Ok = false
	}
	// else {
	// 	c.logger.Println("Map tasks are done - Fetching reduce task")
	// 	for _, task := range c.mapTasks {
	// 		c.logger.Printf("Reduce task found %d", task.Id)
	// 		if task.Status == NotStarted {
	// 			c.logger.Printf("Reduce task found %d", task.Id)
	// 			reply.Type = Reduce
	// 			reply.Args = map[string]string{
	// 				"Index":           fmt.Sprintf("%d", task.Index),
	// 				"IntermediateDir": INTERMEDIATE_DIR,
	// 				"OutputDir":       OUTPUT_DIR, "IntermediateFile": fmt.Sprintf("intermediate-%d", task.Index),
	// 				"OutputFileName": fmt.Sprintf("out-%d", task.Index),
	// 			}
	// 			return nil
	// 		}
	// 	}
	// }
	return nil
}

func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	c.workerMutex.Lock()
	defer c.workerMutex.Unlock()

	worker := Worker{
		Id:        args.Id,
		Status:    args.Status,
		Buckets:   args.Buckets,
		WorkerDir: args.WorkerDir,
	}
	c.workers = append(c.workers, worker)
	reply.Ok = true

	return nil
}
