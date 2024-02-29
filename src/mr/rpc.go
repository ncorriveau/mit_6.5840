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
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type TaskRequest struct {
	WorkerID int
}

type TaskResponse struct {
	Task interface{}
}

type mapTask struct {
	FileName       string // file name
	TaskNumber     int    // task number
	NReduce        int    // number of reduce tasks
	Status         string // 'idle', 'in-progress', 'completed'
	Worker         int    // pid of worker process handling. 0 if not assigned
	StartTime      int64  // time task was assigned
	OutputFilename string // file name for intermediate output
}

type reduceTask struct {
	Name       string // file name
	TaskNumber int    // task number
	Status     string // 'idle', 'in-progress', 'completed'
	Worker     int    // pid of worker process handling. 0 if not assigned
	StartTime  int64  // time task was assigned
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
