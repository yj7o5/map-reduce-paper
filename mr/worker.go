package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

func (x KeyValue) String() string {
	return fmt.Sprintf("Key: %v, Value: %v", x.Key, x.Value)
}

type ByKey []KeyValue

func (x ByKey) Len() int           { return len(x) }
func (x ByKey) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }
func (x ByKey) Less(i, j int) bool { return x[i].Key < x[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func read(filename string) (string, error) {

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v %v", filename, err)
	}
	defer file.Close()

	data, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v %v", file, err)
	}

	return string(data), nil
}

var WorkerId = os.Getpid()

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		reqArgs := RequestTaskArgs{WorkerId}
		reqReply := RequestTaskReply{}

		if succeeded := call("Coordinator.RequestTask", &reqArgs, &reqReply); !succeeded {
			break
		}

		if len(reqReply.Input) == 0 {
			time.Sleep(250 * time.Millisecond)
			continue
		}

		resArgs := ResponseTaskArgs{Type: reqReply.Type, Index: reqReply.Index}
		resReply := ResponseTaskReply{}
		if reqReply.Type == MapTask {
			resArgs.Buckets = doMap(reqReply.Index, reqReply.ReduceN, reqReply.Input[0], mapf)
		} else {
			doReduce(reqReply.Index, reqReply.Input, reducef)
		}

		call("Coordinator.SetTaskCompleted", &resArgs, &resReply)
	}

	log.Printf("Worker shutting down WorkerId=%v\n", WorkerId)
}

//
// Performs the map function on a given set of key-value pairs
//
func doMap(index int, reduceN int, sourceFile string, mapf func(string, string) []KeyValue) []int {
	file, _ := read(sourceFile)

	// apply given map function in the worker definition
	kvps := mapf(sourceFile, file)

	// map of filename to json encoders for each of the intermediate file
	encoders := make(map[string]*json.Encoder)

	tmpSuffix := "-map-tmp"

	indexes := []int{}

	// go through each of the map result key-value pair
	for _, kv := range kvps {
		x := index
		y := ihash(kv.Key) % reduceN

		if !containsInt(y, indexes) {
			indexes = append(indexes, y)
		}

		// create intermediate file with following convention: mr-{mapTaskNumber}-{reduceTaskNumber}
		filename := fmt.Sprintf("mr-%d-%d%v", x, y, tmpSuffix)

		var encoder *json.Encoder
		encoder, ok := encoders[filename]
		if !ok {
			fd, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0777)
			if err != nil {
				log.Fatalln("Error opening file when writing map output", filename, err)
			}
			defer fd.Close()

			encoders[filename] = json.NewEncoder(fd)
			encoder = encoders[filename]
		}

		encoder.Encode(&kv)
	}

	// rename tmp files to actual reduce files
	files, _ := os.ReadDir(".")
	for _, file := range files {
		if strings.HasSuffix(file.Name(), tmpSuffix) {
			reduceFileName := strings.Replace(file.Name(), tmpSuffix, "", 1)
			os.Rename(file.Name(), reduceFileName)
		}
	}

	return indexes
}

//
// Performs the reduce function on given input files
//
func doReduce(index int, files []string, reducef func(string, []string) string) error {
	// process thru all the files under the tmp directory
	kva := []KeyValue{}

	for _, file := range files {
		// open file to reduce to decode key-value from disk for processing

		ifile, err := os.Open(file)
		if err != nil {
			log.Fatalf("Error opening up intermediate file for reduce %v", file)
			panic(err)
		}
		defer ifile.Close()

		decoder := json.NewDecoder(ifile)

		// aggregate all key-value pairs and sort them out before writing to output file
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}

			kva = append(kva, kv)
		}
	}

	// Call reduce on each distinct key in intermediate[]
	ofilename := fmt.Sprintf("mr-out-%v", index)
	ofile, err := os.OpenFile(ofilename, os.O_CREATE|os.O_WRONLY, 0777)
	if err != nil {
		log.Fatalf("Error opening file for writing out reduce output %v", ofilename)
		panic(err)
	}
	defer ofile.Close()

	sort.Sort(ByKey(kva))

	// read each key and associated values to build a reduce output of key value per line
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}

		key := kva[i].Key
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}

		value := reducef(kva[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", key, value)

		i = j
	}

	// notify co-ordinator of the task done
	args := ResponseTaskArgs{Index: index}
	reply := ResponseTaskReply{}
	if ok := call("Coordinator.SetTaskCompleted", &args, &reply); !ok {
		log.Fatalf("Failed reporting reduce task completion to coordinator")
	}

	return nil
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

	log.Println(err)
	return false
}
