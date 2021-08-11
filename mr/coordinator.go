package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"time"
)

type TaskStatus int
type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
)

const (
	UNASSIGNED TaskStatus = iota
	ASSIGNED
	FINISHED
)

type Task struct {
	Type   TaskType
	Status TaskStatus
	Index  int
	File   string
}

type Coordinator struct {
	mu          sync.Mutex
	mapTasks    []*Task
	reduceTasks []*Task
	nReduce     int
}

func getIdleTask(tasks []*Task) *Task {
	for i := 0; i < len(tasks); i++ {
		if tasks[i].Status == UNASSIGNED {
			return tasks[i]
		}
	}
	return nil
}

func (t *Task) getTaskName() string {
	name := "Map"
	if t.Type == ReduceTask {
		name = "Reduce"
	}
	return name
}

//
// Monitors task for completion, if after 10 seconds the task still hasn't been completed
// then mark the task idle or incomplete to be picked up by another worker
//
func MonitorWork(c *Coordinator, task *Task, workerId int) {
	// Wait for 10 seconds and then checking if the certain Task has been completed
	// If not then assume failure and reset the status from RUNNING to IDLE
	time.Sleep(time.Duration(10) * time.Second)

	c.mu.Lock()
	defer c.mu.Unlock()

	if task.Status == ASSIGNED {
		task.Status = UNASSIGNED
		log.Printf("%v task %v failed on worker %v", task.getTaskName(), task.Index, workerId)
	} else {
		log.Printf("%v task %v succeed on worker %v", task.getTaskName(), task.Index, workerId)
	}
}

func allTasksCompleted(tasks []*Task, tt TaskType) bool {
	for _, t := range tasks {
		if t.Status != FINISHED {
			return false
		}
	}
	return true
}

func (c *Coordinator) RequestWork(args *RequestWorkArgs, reply *RequestWorkReply) error {

	// lock the resources on the coordinator
	c.mu.Lock()
	defer c.mu.Unlock()

	// pick an idle map task from coordinator, given the requestor is asking for map
	var task *Task
	if task = getIdleTask(c.mapTasks); task == nil {
		// check if all map tasks have been completed
		if allTasksCompleted(c.mapTasks, MapTask) {
			task = getIdleTask(c.reduceTasks)
		}
	}

	if task != nil {
		task.Status = ASSIGNED

		reply.Type = task.Type
		reply.ReduceN = c.nReduce
		reply.Index = task.Index

		if task.Type == MapTask {
			reply.File = []string{task.File}
		} else {
			files, _ := os.ReadDir(".")
			for _, file := range files {
				name := file.Name()
				if strings.HasSuffix(name, fmt.Sprint(task.Index)) {
					reply.File = append(reply.File, name)
				}
			}
		}

		log.Printf("%v task %v assigned to worker %v\n", task.getTaskName(), task.Index, args.WorkerId)

		go MonitorWork(c, task, args.WorkerId)
	} else {
		log.Println("No task found in an IDLE state")
	}

	return nil
}

func (c *Coordinator) CompleteWork(args *CompleteWorkArgs, reply *CompleteWorkReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var task *Task
	if args.Type == MapTask {
		task = c.mapTasks[args.Index]
	} else {
		task = c.reduceTasks[args.Index]
	}

	task.Status = FINISHED

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

func completedTasks(tasks []*Task) int {
	count := 0
	for _, t := range tasks {
		if t.Status == FINISHED {
			count += 1
		}
	}
	return count
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	cmpMapCnt, cmpRedCnt := completedTasks(c.mapTasks), completedTasks(c.reduceTasks)
	totMapCnt, totRedCnt := len(c.mapTasks), len(c.reduceTasks)

	completed := cmpMapCnt == totMapCnt && cmpRedCnt == totRedCnt

	log.Printf("%v/%v map tasks and %v/%v reduce tasks completed", cmpMapCnt, totMapCnt, cmpRedCnt, totRedCnt)

	return completed
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:     nReduce,
		mapTasks:    make([]*Task, 0, len(files)),
		reduceTasks: make([]*Task, 0, nReduce),
	}

	for i := 0; i < len(files); i++ {
		log.Printf("register map task file=%v index=%v\n", files[i], i)

		mapTask := &Task{MapTask, UNASSIGNED, i, files[i]}
		c.mapTasks = append(c.mapTasks, mapTask)
	}
	for i := 0; i < c.nReduce; i++ {
		log.Printf("register reduce task index=%v\n", i)

		reduceTask := &Task{ReduceTask, UNASSIGNED, i, ""}
		c.reduceTasks = append(c.reduceTasks, reduceTask)
	}

	c.server()

	return &c
}
