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
)

type Task struct {
	status TaskStatus
	bucket int
	input  []string
}

type Coordinator struct {
	mu    sync.Mutex
	tasks []*Task

	mapTasks    []*Task
	reduceTasks []*Task
	nReduce     int
}

//
// Monitors task for completion, if after 10 seconds the task still hasn't been completed
// then mark the task idle or incomplete to be picked up by another worker
//
func monitorWorker(c *Coordinator, bucket int, bType TaskType, workerId int) {
	// Wait for 10 seconds and then checking if the certain Task has been completed
	// If not then assume failure and reset the status from RUNNING to IDLE
	time.Sleep(time.Duration(10) * time.Second)

	c.mu.Lock()
	defer c.mu.Unlock()

	var task *Task
	if bType == MapTask {
		for _, t := range c.mapTasks {
			if t.bucket == bucket {
				task = t
				break
			}
		}
	} else {
		for _, t := range c.reduceTasks {
			if t.bucket == bucket {
				task = t
				break
			}
		}
	}

	// If task still exists assume it has failed therefore reset to unassigned
	if task != nil {
		task.status = UNASSIGNED
		log.Printf("%v task %v failed on worker %v\n", bType, bucket, workerId)
	} else {
		log.Printf("%v task %v succeed on worker %v\n", bType, bucket, workerId)
	}
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {

	// lock the resources on the coordinator
	c.mu.Lock()
	defer c.mu.Unlock()

	// pick an idle worker(reduce if no map task left)
	var workerTask *Task
	var workerTaskType TaskType
	for _, t := range c.mapTasks {
		if t.status == UNASSIGNED {
			workerTask = t
			workerTaskType = MapTask
			break
		}
	}

	// now pick a reduce task given no unassigned map task was found
	canReduce := workerTask == nil && len(c.mapTasks) == 0
	if canReduce {
		// search for an unassigned reduce task
		for _, t := range c.reduceTasks {
			if t.status == UNASSIGNED {
				workerTask = t
				workerTaskType = ReduceTask
				break
			}
		}
	}

	// if we still don't find task then either:
	// 	a) some or all maps have been completed or currently being worked on, therefore reduce can't start
	// 	b) some or all reduces have started or completed but none to be assigned, yet
	if workerTask != nil {
		workerTask.status = ASSIGNED

		input := workerTask.input
		if workerTaskType == ReduceTask {
			rinput := []string{}
			dir, _ := os.ReadDir(".")
			for _, f := range dir {
				if strings.HasSuffix(f.Name(), fmt.Sprint(workerTask.bucket)) {
					rinput = append(rinput, f.Name())
				}
			}
			fmt.Printf("assign reduce bucket %v task for files %v\n", workerTask.bucket, rinput)
			input = rinput
		}

		reply.Type = workerTaskType
		reply.Input = input
		reply.Index = workerTask.bucket
		reply.ReduceN = c.nReduce

		go monitorWorker(c, workerTask.bucket, workerTaskType, args.WorkerId)

		return nil
	}

	log.Println("no task unassigned found in the queue")

	return nil
}

func (c *Coordinator) SetTaskCompleted(args *ResponseTaskArgs, reply *ResponseTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// if the completed task was a map task, then we need to add reduce task buckets
	if args.Type == MapTask {
		// select the map task index that it corresponds to
		mIndex := -1

		for i, t := range c.mapTasks {
			if t.bucket == args.Index {
				mIndex = i
				break
			}
		}

		// first check if this map task has already been proceed(i.e. removed from slice) or,
		// has been marked as UNASSIGNED which possibly due to timeout then bail out early
		if mIndex == -1 || c.mapTasks[mIndex].status == UNASSIGNED {
			return nil
		}

		// remove the map task given that it has been completed
		c.mapTasks = append(c.mapTasks[:mIndex], c.mapTasks[mIndex+1:]...) // TODO: this is expensive, reshifting

		// for each bucket arrange the reduce task and append to reduce tasks list with input files
		// for reduce operations
		for _, bucket := range args.Buckets {
			hasBucket := false
			// ensure we have added this bucket in the reduce
			for _, t := range c.reduceTasks {
				if !hasBucket && t.bucket == bucket {
					hasBucket = true
				}
			}

			// if bucket doesn't exist then select all the files for this reduce bucket
			if !hasBucket {
				c.reduceTasks = append(c.reduceTasks, &Task{bucket: bucket})
			}
		}

		log.Printf("searching for map tasks with index=%v map_buckets=%v reduce_buckets=%v\n", args.Index, len(c.mapTasks), len(c.reduceTasks))
	} else {
		// for reduce task completed we just remove the reduce task from the list
		rIndex := -1
		for i, t := range c.reduceTasks {
			if t.bucket == args.Index {
				rIndex = i
				break
			}
		}

		c.reduceTasks = append(c.reduceTasks[:rIndex], c.reduceTasks[rIndex+1:]...)
	}

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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	nmap, nreduce := len(c.mapTasks), len(c.reduceTasks)
	done := nmap+nreduce == 0

	log.Printf("%v/%v map/reduce tasks left", nmap, nreduce)

	return done
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce: nReduce,
		tasks:   make([]*Task, 0, len(files)+nReduce),

		mapTasks:    make([]*Task, 0, len(files)),
		reduceTasks: make([]*Task, 0, nReduce),
	}

	for i := 0; i < len(files); i++ {
		log.Printf("register map task file=%v index=%v\n", files[i], i)

		c.mapTasks = append(c.mapTasks, &Task{UNASSIGNED, i, []string{files[i]}})
	}

	c.server()

	return &c
}
