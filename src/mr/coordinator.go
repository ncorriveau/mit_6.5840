package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	MapTasks    []Task
	ReduceTasks []Task

	//TODO: add in the status struct here
	mapTaskStatus    map[int]string
	reduceTaskStatus map[int]string
	mapStartTimes    map[int]time.Time
	reduceStartTimes map[int]time.Time

	//TODO: change this to map
	IntermediateFiles []string // intermediate files for reduce tasks
	mapDone           int      // number of map tasks completed
	reduceDone        int      // number of reduce tasks completed
	mu                sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) getMapTaskStatus(taskNumber int) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mapTaskStatus[taskNumber]
}

func (c *Coordinator) AssignTask(args *TaskRequest, reply *TaskResponse) error {
	// log.Printf("Assigning task to worker %d", args.WorkerID)
	// lock given shared structures
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.mapDone != len(c.MapTasks) {
		for _, task := range c.MapTasks {
			if c.mapTaskStatus[task.TaskNumber] == "idle" {
				c.mapTaskStatus[task.TaskNumber] = "in-progress"
				c.mapStartTimes[task.TaskNumber] = time.Now()
				reply.Task = task
				return nil
			}
		}
	} else {
		for _, task := range c.ReduceTasks {
			if c.reduceTaskStatus[task.TaskNumber] == "idle" {
				c.reduceTaskStatus[task.TaskNumber] = "in-progress"
				c.reduceStartTimes[task.TaskNumber] = time.Now()
				task.Filenames = c.IntermediateFiles
				reply.Task = task
				// add task monitor

				return nil
			}
		}
	}
	return nil
}

func (c *Coordinator) TaskDone(args *TaskDoneRequest, reply *TaskDoneResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	task := args.Task
	if task.TaskType == "map" {
		c.mapTaskStatus[task.TaskNumber] = "completed"
		c.mapDone++
		c.IntermediateFiles = append(c.IntermediateFiles, args.OutputFilenames...)
		delete(c.mapStartTimes, task.TaskNumber)
		reply.Success = true
		return nil

	} else if task.TaskType == "reduce" {
		c.reduceTaskStatus[task.TaskNumber] = "completed"
		c.reduceDone++
		delete(c.reduceStartTimes, task.TaskNumber)
		reply.Success = true
		return nil
	}
	return nil
}

func (c *Coordinator) MonitorTasks() {
	for {
		c.mu.Lock()
		now := time.Now()
		if c.mapDone != len(c.MapTasks) {
			for taskID, startTime := range c.mapStartTimes {
				if now.Sub(startTime) > 10*time.Second {
					// log.Printf("Map Task %d has been running for more than 10 seconds", taskID)
					c.mapTaskStatus[taskID] = "idle"
					c.mapStartTimes[taskID] = time.Time{}
				}
			}
		} else if c.reduceDone != len(c.ReduceTasks) {
			for taskID, startTime := range c.reduceStartTimes {
				if now.Sub(startTime) > 10*time.Second {
					// log.Printf("Reduce task %d has been running for more than 10 seconds", taskID)
					c.reduceTaskStatus[taskID] = "idle"
					c.reduceStartTimes[taskID] = time.Time{}
				}
			}
		}
		c.mu.Unlock()

		// Sleep for a bit before checking again
		time.Sleep(1 * time.Second)
	}
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
	c := Coordinator{mapTaskStatus: make(map[int]string), reduceTaskStatus: make(map[int]string), mapStartTimes: make(map[int]time.Time), reduceStartTimes: make(map[int]time.Time)}
	// Your code here.
	for i, file := range files {
		c.MapTasks = append(c.MapTasks, Task{
			Filenames:  []string{file},
			TaskNumber: i,
			NReduce:    nReduce,
			TaskType:   "map",
		})
		c.mapTaskStatus[i] = "idle"
	}

	for i := 0; i < nReduce; i++ {
		c.ReduceTasks = append(c.ReduceTasks, Task{
			TaskNumber: i,
			NReduce:    nReduce,
			TaskType:   "reduce",
		})
		c.reduceTaskStatus[i] = "idle"
	}
	go c.MonitorTasks()
	c.server()
	return &c
}
