package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

type mapfType func(string, string) []KeyValue
type reducefType func(string, []string) string

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf mapfType, reducef reducefType) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	isReduce, reduceTask, mapTask := callGetTask()
	if isReduce {
		if reduceTask == nil {
			log.Fatalf("received nil key for reduce task")
		}
		execReduce(reducef, reduceTask.Key)
	} else {
		if mapTask == nil {
			log.Fatalf("received nil filename for map task")
		}
		execMap(mapf, mapTask)
	}

}

func execMap(mapf mapfType, mapTask *MapTask) {
	// Perform map on input file
	file, err := os.Open(mapTask.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", mapTask.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", mapTask.Filename)
	}
	file.Close()
	kva := mapf(mapTask.Filename, string(content))

	// Bucket intermediate key-values
	intermediate := make(map[int][]KeyValue)
	for _, kv := range kva {
		reduceID := ihash(kv.Key) % mapTask.NReduce
		intermediate[reduceID] = append(intermediate[reduceID], kv)
	}

	// Dump key-values to intermediate files
	for i := 0; i < mapTask.NReduce; i++ {
		// Create intermediate file
		iname := fmt.Sprintf("mr-%v-%v", mapTask.ID, i)
		ifile, err := os.Create(iname)
		if err != nil {
			log.Fatal("cannot create intermediate file:", err)
		}
		// Encode to JSON and write to file
		enc := json.NewEncoder(ifile)
		for _, kv := range intermediate[i] {
			if err := enc.Encode(&kv); err != nil {
				log.Fatal("failure encoding intermediate data to json:", err)
			}
		}
	}
}

// Reduce TODO
func execReduce(reducef reducefType, key string) {
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func callGetTask() (bool, *ReduceTask, *MapTask) {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	call("Master.GetTask", &args, &reply)
	return reply.IsReduce, reply.RTask, reply.MTask
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
