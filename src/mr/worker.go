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
)

// Map/Reduce type signatures
type mapfType func(string, string) []KeyValue
type reducefType func(string, []string) string

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	for {
		isDone, isReduce, reduceTask, mapTask := callGetTask()
		if isDone {
			break
		}
		if isReduce {
			if reduceTask == nil {
				log.Fatal("received nil reduce task")
			}
			execReduce(reducef, reduceTask)
		} else {
			if mapTask == nil {
				log.Fatal("received nil map task")
			}
			execMap(mapf, mapTask)
		}
		callPostCompletion(isReduce, reduceTask, mapTask)
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
	// TODO: Use temporary names and rename on completion (not necessary tho)
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

func execReduce(reducef reducefType, reduceTask *ReduceTask) {
	// Shuffle
	intermediate := []KeyValue{}
	for i := 0; i < reduceTask.NMap; i++ {
		iname := fmt.Sprintf("mr-%v-%v", i, reduceTask.ID)
		ifile, err := os.Open(iname)
		if err != nil {
			log.Fatal("cannot open intermediate file:", err)
		}
		dec := json.NewDecoder(ifile)
		for {
			kv := KeyValue{}
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	// Sort
	sort.Sort(ByKey(intermediate))

	// Reduce
	oname := fmt.Sprintf("mr-out-%v", reduceTask.ID)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
}

func callGetTask() (bool, bool, *ReduceTask, *MapTask) {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	call("Master.GetTask", &args, &reply)
	return reply.IsDone, reply.IsReduce, reply.RTask, reply.MTask
}

func callPostCompletion(isReduce bool, reduceTask *ReduceTask, mapTask *MapTask) {
	args := PostCompletionArgs{IsReduce: isReduce, RTask: reduceTask, MTask: mapTask}
	reply := PostCompletionReply{}
	call("Master.PostCompletion", &args, &reply)
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
