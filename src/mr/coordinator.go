package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	MapTasks          []mapTask
	ReduceTasks       []reduceTask
	IntermediateFiles []string
	mapDone           int // number of map tasks completed
	reduceDone        int // number of reduce tasks completed
	mu                sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
// func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }

func (c *Coordinator) AssignTask(args *TaskRequest, reply *TaskResponse) error {
	// log.Printf("Assigning task to worker %d", args.WorkerID)
	// lock given shared structures
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.mapDone != len(c.MapTasks) {
		for i, task := range c.MapTasks {
			if task.Status == "idle" {
				c.MapTasks[i].Status = "in-progress"
				c.MapTasks[i].Worker = args.WorkerID
				reply.Task = task
				return nil
			}
		}
	} else {
		for i, task := range c.ReduceTasks {
			if task.Status == "idle" {
				c.ReduceTasks[i].Status = "in-progress"
				c.ReduceTasks[i].Worker = args.WorkerID
				task.IntermediateFiles = c.IntermediateFiles
				reply.Task = task

				return nil
			}
		}
	}
	return nil
}

func (c *Coordinator) TaskDone(args *TaskDoneRequest, reply *TaskDoneResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == "map" {
		for i, task := range c.MapTasks {
			if task.TaskNumber == args.TaskNumber {
				c.MapTasks[i].Status = "completed"
				// record all output file locations on the coordinator
				for _, file := range args.OutputFilenames {
					c.IntermediateFiles = append(c.IntermediateFiles, file)
				}
				c.mapDone++
				reply.Success = true
				log.Printf("Task completed: %d", c.mapDone)
				return nil
			}
		}
	} else if args.TaskType == "reduce" {
		for i, task := range c.ReduceTasks {
			if task.TaskNumber == args.TaskNumber {
				c.ReduceTasks[i].Status = "completed"
				c.reduceDone++
				reply.Success = true
				log.Printf("Task completed: %d", c.reduceDone)
				return nil
			}
		}
	}
	return nil
}

func (c *Coordinator) AllDone(args *AllDoneRequest, reply *AllDoneResponse) error {
	if c.mapDone == len(c.MapTasks) && c.reduceDone == len(c.ReduceTasks) {
		reply.Success = true
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
	if c.mapDone == len(c.MapTasks) && c.reduceDone == len(c.ReduceTasks) {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	for i, file := range files {
		c.MapTasks = append(c.MapTasks, mapTask{
			FileName:   file,
			TaskNumber: i,
			NReduce:    nReduce,
			Status:     "idle",
			Worker:     0,
		})
	}

	for i := 0; i < nReduce; i++ {
		c.ReduceTasks = append(c.ReduceTasks, reduceTask{
			TaskNumber: i,
			Status:     "idle",
			Worker:     0,
		})
	}

	c.server()
	return &c
}
