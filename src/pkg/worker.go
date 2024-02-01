package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	pid := os.Getpid()
	workerDir := fmt.Sprintf("/tmp/%d", pid)
	err := os.Mkdir(workerDir, 0755)
	if err != nil {
		log.Fatal("Error creating worker directory", err)
	}

	registerWorker()

	for true {
		task, err := GetTask()

		if err != nil {
			log.Println("No Tasks Found - Sleeping")
			time.Sleep(time.Second)
			continue
		}

		switch task.GetTaskType() {
		case Map:
			{
				task := task.(MapTask)
				task.Status = Running
				log.Printf("Received map task ID %d\n", task.Id)

				intermediateDir := fmt.Sprintf("%s/%d", workerDir, task.Id)

				err := os.Mkdir(intermediateDir, 0755)
				if err != nil {
					log.Printf("Error creating intermediate directory for Task ID %d", task.Id)
					log.Fatal(err)
				}

				files := CreateOutputFiles(BUCKETS, intermediateDir)
				content := ReadFile(task.InputFile)
				keyValues := mapf(task.InputFile, content)
				for _, kv := range keyValues {
					bucket := ihash(kv.Key) % BUCKETS
					file := files[bucket]
					file.Write(kv.Key, kv.Value)

				}
				log.Printf("Map task ID %d completed\n", task.Id)
				task.SetStatus(Success)
				SendTaskCompletion(task)
			}
		case Reduce:
			{
				task := task.(ReduceTask)
				task.Status = Running
				log.Println("Received reduce task\n", task)
			}
		}
		// case ReducePhase:
		// 	{
		// 		task.Status = Running
		// 		args := task.Args.(ReduceArgs)
		// 		outputFile, _ := os.Create(fmt.Sprintf("%s/mr-out-%d", args.OutputDir, args.Index))
		// 		intermediate := LoadDataFromIntermediateFile(args.IntermediateDir + args.IntermediateFile)
		// 		sort.Sort(ByKey(intermediate))

		// 		i := 0
		// 		for i < len(intermediate) {
		// 			j := i + 1
		// 			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
		// 				j++
		// 			}
		// 			values := []string{}
		// 			for k := i; k < j; k++ {
		// 				values = append(values, intermediate[k].Value)
		// 			}
		// 			output := reducef(intermediate[i].Key, values)

		// 			WriteRecord(outputFile, intermediate[i].Key, output)

		// 			i = j
		// 		}
		// 		outputFile.Close()
		// 	}
		// }
		// task.Status = Success
	}
}

func GetTask() (Task, error) {
	args := NewTaskArgs{}
	reply := NewTaskReply{}
	call("Coordinator.SendTask", &args, &reply)
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
			Id:               id,
			Status:           NotStarted,
			Index:            index,
			IntermediateDir:  args["IntermediateDir"],
			OutputDir:        args["OutputDir"],
			IntermediateFile: args["IntermediateFile"],
			OutputFileName:   args["OutputFileName"],
			Type:             Reduce,
		}
		return task, nil
	}
	return nil, fmt.Errorf("Unknown task type")
}

func SendTaskCompletion(task Task) {
	args := TaskStatusArgs{
		Id:     task.GetId(),
		Status: task.GetStatus(),
		Type:   task.GetTaskType(),
	}
	reply := TaskStatusReply{}
	call("Coordinator.GetTaskStatus", &args, &reply)

	if !reply.Ok {
		fmt.Println("Task completion failed")
	}
}

func registerWorker() {
	workerId := os.Getpid()
	args := RegisterWorkerArgs{workerId}
	reply := RegisterWorkerReply{}
	call("Coordinator.RegisterWorker", &args, &reply)

	if reply.Ok {
		fmt.Printf("Worker %d registered successfully\n", workerId)
	} else {
		fmt.Printf("Worker %d registration failed\n", workerId)
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
