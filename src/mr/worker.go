package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// Your worker implementation here.
	// loop and ask for tasks from the coordinator
	// and execute them
	// save down and alert to coordinator when done

	var nReduce, taskNum int
	workerId := os.Getpid()
	intermediate := []KeyValue{}

	_, myTask := GetTask(workerId)
	log.Print("Got task")

	if mt, ok := myTask.Task.(mapTask); ok {
		nReduce = mt.NReduce
		taskNum = mt.TaskNumber

		file, err := os.Open(mt.FileName)
		if err != nil {
			log.Fatalf("cannot open %v", mt.FileName)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", mt.FileName)
		}
		file.Close()
		kva := mapf(mt.FileName, string(content))
		intermediate = append(intermediate, kva...)
	}

	// loop thru k,v pairs and write
	fileDescriptors := make(map[int]*os.File)
	fileNames := []string{}

	for _, kv := range intermediate {
		reduceNumber := ihash(kv.Key) % nReduce
		filename := fmt.Sprintf("mr-%d-%d", taskNum, reduceNumber)
		fileNames = append(fileNames, filename)

		if _, ok := fileDescriptors[reduceNumber]; !ok {
			file, err := os.Create(filename)
			if err != nil {
				log.Fatalf("cannot create %v", filename)
			}
			fileDescriptors[reduceNumber] = file
		}

		// Write the key-value pair to the appropriate file
		enc := json.NewEncoder(fileDescriptors[reduceNumber])
		err := enc.Encode(&kv)
		if err != nil {
			fmt.Println("Error writing to file:", err)
			return
		}
	}
	// Close all the files
	for _, file := range fileDescriptors {
		err := file.Close()
		if err != nil {
			fmt.Println("Error closing file:", err)
		}
	}
	log.Print("Finished writing files")
	// call coordinator to say done and send filenames
	_, done := DoneTask(workerId, taskNum, fileNames)

	if !done.Success {
		log.Print("Task not done")
	}
	log.Printf("Worker % d done, waiting for next task", workerId)
}

func GetTask(id int) (bool, TaskResponse) {
	args := TaskRequest{WorkerID: id}
	reply := TaskResponse{} // set with default values
	log.Print("Calling Coordinator.AssignTask")
	ok := call("Coordinator.AssignTask", &args, &reply)
	// TODO add some error handling probably
	return ok, reply
}

func DoneTask(id int, taskNum int, filenames []string) (bool, TaskDoneResponse) {
	args := TaskDoneRequest{WorkerID: id, TaskNumber: taskNum, OutputFilenames: filenames}
	reply := TaskDoneResponse{}
	log.Print("Calling Coordinator.TaskDone")
	ok := call("Coordinator.TaskDone", &args, &reply)
	// TODO add some error handling probably
	return ok, reply
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//

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
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
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
