package mr

import (
	"encoding/gob"
	"os"
	"strconv"
)

//
// RPC definitions.
//
// remember to capitalize all names.

// Add your RPC definitions here.
type TaskRequest struct {
	WorkerID int
}

type TaskResponse struct {
	Task interface{}
}

type TaskDoneRequest struct {
	WorkerID        int      // pid of worker process
	TaskType        string   // 'map' or 'reduce'
	TaskNumber      int      // task number
	OutputFilenames []string // file location of intermediate outputs
}

type TaskDoneResponse struct {
	Success bool
}

type AllDoneRequest struct {
	WorkerID int
}

type AllDoneResponse struct {
	Success bool
}

type mapTask struct {
	FileName   string // file name
	TaskNumber int    // task number
	NReduce    int    // number of reduce tasks
	Status     string // 'idle', 'in-progress', 'completed'
	Worker     int    // pid of worker process handling. 0 if not assigned
	// potentiall add start time and end time
}

type reduceTask struct {
	IntermediateFiles []string // file name
	TaskNumber        int      // task number
	Status            string   // 'idle', 'in-progress', 'completed'
	Worker            int      // pid of worker process handling. 0 if not assigned
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func init() {
	gob.Register(mapTask{})
	gob.Register(reduceTask{})
}
