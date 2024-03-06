package mr

import (
	"os"
	"strconv"
)

//
// RPC definitions.
//
// remember to capitalize all names.

// Add your RPC definitions here.
type Task struct {
	Filenames  []string // Adjusted from Filename to Filenames
	NReduce    int
	TaskNumber int
	TaskType   string // "map" or "reduce"
}

type TaskRequest struct {
	WorkerID int
}

type TaskResponse struct {
	Task Task
}
type TaskDoneRequest struct {
	Task            Task
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

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
