package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Master struct {
	// Your definitions here.
	files         []string
	nReduce       int
	isReducePhase bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	if m.isReducePhase {
		reply.IsReduce = true
		reply.RTask = &ReduceTask{ID: 0, NMap: 1}
		return nil
	}
	reply.IsReduce = false
	reply.MTask = &MapTask{Filename: m.files[0], ID: 0, NReduce: m.nReduce}
	m.isReducePhase = true
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{files: files, nReduce: nReduce}

	// Your code here.

	m.server()
	return &m
}
