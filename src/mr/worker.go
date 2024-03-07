package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"regexp"
	"sort"
	"strconv"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// HELPERS //
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func executeMapTask(task Task, mapf func(string, string) []KeyValue) {
	filename := task.Filenames[0]
	intermediate := []KeyValue{}

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}

	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	// loop thru k,v pairs and write
	// fileDescriptors := make(map[int]*os.File)
	// fileSet := make(map[string]bool)
	intermediateFiles := make([]string, task.NReduce)
	for i := 0; i < task.NReduce; i++ {
		tempFile, err := os.CreateTemp("", fmt.Sprintf("mr-%d-%d-*", task.TaskNumber, i))
		if err != nil {
			log.Fatalf("cannot create temp file for reduce task %d", i)
		}

		enc := json.NewEncoder(tempFile)
		for _, kv := range kva {
			if ihash(kv.Key)%task.NReduce == i {
				if err := enc.Encode(&kv); err != nil {
					log.Fatalf("cannot encode kv: %v", err)
				}
			}
		}

		tempFile.Close()
		finalFilename := fmt.Sprintf("mr-%d-%d", task.TaskNumber, i)
		// Rename the temp file to the final filename
		if err := os.Rename(tempFile.Name(), finalFilename); err != nil {
			log.Fatalf("cannot rename temp file: %v", err)
		}

		// Record the name of the final file
		intermediateFiles[i] = finalFilename

	}
	log.Print("Finished writing files")

	// call coordinator to say done and send filenames
	_, done := DoneTask(task, intermediateFiles)

	if !done.Success {
		log.Print("Task not done")
	}
	log.Printf("Map task % d done, waiting for next task", task.TaskNumber)
}

func executeReduceTask(task Task, reducef func(string, []string) string) {
	// read in all the intermediate files for this reduce task
	// add all k,v pairs to a slice we can then sort
	var kva []KeyValue
	strTaskNum := strconv.Itoa(task.TaskNumber)
	re := regexp.MustCompile(`mr-\d+-` + regexp.QuoteMeta(strTaskNum) + `$`)

	// extract file names that end in the task number
	for _, filename := range task.Filenames {
		if re.MatchString(filename) {
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}

			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}
			file.Close()
		}
	}

	// sort intermediate key-value pairs and create output file
	sort.Sort(ByKey(kva))
	oname := fmt.Sprintf("mr-out-%d", task.TaskNumber)

	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("cannot create %v", oname)
	}
	defer ofile.Close()

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		_, err := fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		if err != nil {
			log.Fatalf("error writing to %v: %v", oname, err)
		}
		i = j // Move to next group of keys

	}
	_, done := DoneTask(task, []string{oname})
	if !done.Success {
		log.Print("Task not done")
	}
	log.Printf("Reduce task % d done, waiting for next task", task.TaskNumber)
}

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// Your worker implementation here.
	// loop and ask for tasks from the coordinator
	// and execute them
	// save down and alert to coordinator when done
	workerId := os.Getpid()
	for {
		_, allDoneResponse := CheckDone(workerId)
		if allDoneResponse.Success {
			log.Printf("All tasks done, worker %d exiting", workerId)
			break
		}

		_, myTask := GetTask(workerId)

		if myTask.Task.TaskType == "map" {
			log.Printf("Starting Map task %d", myTask.Task.TaskNumber)
			executeMapTask(myTask.Task, mapf)

		} else if myTask.Task.TaskType == "reduce" {
			log.Printf("Starting Reduce task %d", myTask.Task.TaskNumber)
			executeReduceTask(myTask.Task, reducef)
		}
	}
}

func GetTask(id int) (bool, TaskResponse) {
	args := TaskRequest{WorkerID: id}
	reply := TaskResponse{} // set with default values
	// log.Print("Calling Coordinator.AssignTask")
	ok := call("Coordinator.AssignTask", &args, &reply)
	// TODO add some error handling probably
	return ok, reply
}

func DoneTask(task Task, outputFiles []string) (bool, TaskDoneResponse) {
	args := TaskDoneRequest{Task: task, OutputFilenames: outputFiles}
	reply := TaskDoneResponse{}
	ok := call("Coordinator.TaskDone", &args, &reply)
	// TODO add some error handling probably
	return ok, reply
}

func CheckDone(id int) (bool, AllDoneResponse) {
	args := AllDoneRequest{WorkerID: id}
	reply := AllDoneResponse{}
	ok := call("Coordinator.AllDone", &args, &reply)
	// TODO add some error handling probably
	return ok, reply
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
