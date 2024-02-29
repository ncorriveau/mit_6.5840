package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// Your definitions here.
	MapTasks    []mapTask
	ReduceTasks []reduceTask
	mapDone     int // number of map tasks completed
	reduceDone  int // number of reduce tasks completed

}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AssignTask(args *TaskRequest, reply *TaskResponse) error {
	// assign a task to the worker
	// if there are no tasks left, return an error
	// else return the task
	for i, task := range c.MapTasks {
		if task.Status == "idle" {
			c.MapTasks[i].Status = "in-progress"
			c.MapTasks[i].Worker = args.WorkerID
			c.MapTasks[i].StartTime = 0
			reply.Task = task
			return nil
		}
	}
	for i, task := range c.ReduceTasks {
		if task.Status == "idle" {
			c.ReduceTasks[i].Status = "in-progress"
			c.ReduceTasks[i].Worker = args.WorkerID
			c.ReduceTasks[i].StartTime = 0
			reply.Task = task
			return nil
		}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	for _, file := range files {
		c.MapTasks = append(c.MapTasks, mapTask{Name: file, Status: "idle",
			Worker: 0, StartTime: 0, OutputFilename: ""})
	}

	c.server()
	return &c
}
