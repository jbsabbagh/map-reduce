package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Coordinator struct {
	Logger      *log.Logger
	dataDir     string
	workers     []RegisteredWorker
	mapTasks    []MapTask
	reduceTasks []ReduceTask
	mutex       *sync.RWMutex
}

type RegisteredWorker struct {
	worker        Worker
	lastHeartbeat time.Time
}

func (w RegisteredWorker) GetTask() Task {
	return w.worker.AssignedTask
}

func (w RegisteredWorker) GetId() int {
	return w.worker.Id
}

func (w *RegisteredWorker) AssignTask(task Task) {
	w.worker.AssignedTask = task
}

func (w *RegisteredWorker) SetWorkerStatus(status WorkerStatus) {
	w.worker.Status = status
}

func (w RegisteredWorker) GetWorkerDir() string {
	return w.worker.WorkerDir
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
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if c.mapTasksAreDone() && c.reduceTasksAreDone() {
		c.Logger.Println("All tasks are done!")
		return true
	} else {
		return false
	}
}

func (c *Coordinator) mapTasksAreDone() bool {
	for _, task := range c.mapTasks {
		if !task.IsSuccess() {
			return false
		}
	}
	return true
}

func (c *Coordinator) reduceTasksAreDone() bool {
	for _, task := range c.reduceTasks {
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
	dataDir := filepath.Dir(files[0])
	c := Coordinator{
		mapTasks:    make([]MapTask, 0),
		reduceTasks: make([]ReduceTask, 0),
		dataDir:     dataDir,
		workers:     []RegisteredWorker{},
		Logger:      log.New(os.Stdout, "Coordinator: ", log.Lshortfile|log.Ltime|log.Ldate),
		mutex:       &sync.RWMutex{},
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

	for index := 0; index < nReduce; index++ {
		oname := fmt.Sprintf("out-%d", index)

		reduceTask := ReduceTask{
			Id:             index,
			Status:         NotStarted,
			Index:          index,
			OutputDir:      fmt.Sprintf("%s/out", dataDir),
			OutputFileName: oname,
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
	// c.workerMutex.Lock()
	// defer c.workerMutex.Unlock()

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
	// for _, worker := range c.workers {
	// 	c.Logger.Println(worker)
	// }
	c.Logger.Printf("Completed tasks %d", completedTasks)
	c.Logger.Printf("Total tasks %d", totalTasks)
}

func (c *Coordinator) GetTaskStatus(args *TaskStatusArgs, reply *TaskStatusReply) error {
	id := args.Id
	taskType := args.Type
	reply.Ok = true

	c.Logger.Printf("Received task status for id %d", id)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.setTaskStatus(id, taskType, args.Status)
	return nil
}

func (c *Coordinator) setTaskStatus(id int, taskType TaskType, status TaskStatus) {

	switch taskType {
	case Map:
		{
			for index := range c.mapTasks {
				if c.mapTasks[index].Id == id {
					log.Printf("Changing Map Task ID %d to %d", id, status)
					c.mapTasks[index].SetStatus(status) // Set the status of the task
				}
			}

		}
	case Reduce:
		{
			for index := range c.reduceTasks {
				if c.reduceTasks[index].Id == id {
					log.Printf("Changing Reduce Task ID %d to %d", id, status)
					c.reduceTasks[index].SetStatus(status) // Set the status of the task
				}
			}
		}
	}
}

func (c *Coordinator) SendTask(args *NewTaskArgs, reply *NewTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.Logger.Printf("Worker %d requested a task", args.WorkerId)

	for _, tasks := range c.mapTasks {
		log.Printf("Map Task ID %d Status %s", tasks.Id, tasks.GetStatusStr())
	}

	for _, tasks := range c.reduceTasks {
		log.Printf("Reduce Task ID %d Status %s", tasks.Id, tasks.GetStatusStr())
	}

	if !c.mapTasksAreDone() {
		c.Logger.Println("Map tasks are not done - Fetching map task")
		for index, task := range c.mapTasks {
			if task.Status == NotStarted {
				c.mapTasks[index].SetStatus(Running)
				c.Logger.Printf("Map task found %d\n", task.Id)
				c.assignTaskToWorker(args.WorkerId, task)
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
	} else if !c.reduceTasksAreDone() {

		c.Logger.Println("Map tasks are done - Fetching reduce task")
		for index, task := range c.reduceTasks {
			if task.Status == NotStarted {
				c.reduceTasks[index].SetStatus(Running)
				c.Logger.Printf("Reduce task found %d", task.Id)
				reply.Type = Reduce
				reply.Ok = true
				reply.Args = map[string]string{
					"Id":               fmt.Sprintf("%d", task.Id),
					"Index":            fmt.Sprintf("%d", task.Index),
					"OutputDir":        task.OutputDir,
					"IntermediateFile": fmt.Sprintf("intermediate-%d", task.Index),
					"OutputFileName":   fmt.Sprintf("out-%d", task.Index),
				}
				reply.WorkerDirs = []string{}
				for _, worker := range c.workers {
					reply.WorkerDirs = append(reply.WorkerDirs, worker.GetWorkerDir())
				}
				return nil
			}

		}
	} else {
		c.Logger.Println("All tasks are done!")
		reply.Ok = false
		return nil
	}
	reply.Ok = false
	return nil
}

func (c *Coordinator) assignTaskToWorker(workerId int, task Task) {
	// TODO: better error handling
	for index, worker := range c.workers {
		if worker.GetId() == workerId {
			c.workers[index].AssignTask(task)
		}
	}
}

func (c *Coordinator) CheckWorkerStatus() {
	timeout := 5 * time.Second        // TODO: Make this configurable
	heartbeatCheck := 5 * time.Second // TODO: Make this configurable

	log.Println("Setting Hearbeat Check to 5 seconds and timeout to 5 seconds.")
	go func(timeout, heartbeatCheck time.Duration) {
		for true {
			c.Logger.Println("Heartbeat Check - Checking worker status")
			c.Logger.Printf("Number of workers: %d", len(c.workers))
			c.mutex.Lock()
			for index, worker := range c.workers {
				lastHearbeat := worker.lastHeartbeat
				c.Logger.Printf("Time since last heartbeat for worker %d is %s", worker.worker.Id, time.Since(lastHearbeat))
				if worker.worker.Status != Dead && time.Since(lastHearbeat) > timeout {
					c.Logger.Printf("Worker %d is not responding. Assuming it's dead.", worker.worker.Id)

					c.workers[index].SetWorkerStatus(Dead)

					assignedTask := worker.GetTask()
					if assignedTask != nil {
						c.Logger.Printf("Changing Task ID %d back to Not Started", assignedTask.GetId())
						c.setTaskStatus(assignedTask.GetId(), assignedTask.GetTaskType(), NotStarted)
					}
				}
			}
			c.mutex.Unlock()
			time.Sleep(heartbeatCheck)
		}
	}(timeout, heartbeatCheck)
}

func (c *Coordinator) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for index, worker := range c.workers {
		if worker.worker.Id == args.WorkerId {
			c.Logger.Printf("Received heartbeat from worker %d", args.WorkerId)

			c.workers[index].lastHeartbeat = args.Heartbeat

			reply.Ok = true
			return nil
		}
	}
	reply.Ok = false
	return nil
}

func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	worker := Worker{
		Id:        args.Id,
		Status:    args.Status,
		Buckets:   args.Buckets,
		WorkerDir: args.WorkerDir,
	}
	c.workers = append(c.workers, RegisteredWorker{worker: worker, lastHeartbeat: time.Now()})
	reply.Ok = true

	return nil
}
