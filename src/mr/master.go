package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type phase int

const (
	mapPhase    = 100
	reducePhase = 200
	completed   = 300
)

type Master struct {
	// Your definitions here.
	files        []string
	nReduce      int
	currentPhase phase
	// Temporary method to track issued tasks
	mapCount    int
	reduceCount int
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	switch m.currentPhase {
	case mapPhase:
		reply.IsReduce = false
		reply.MTask = &MapTask{Filename: m.files[m.mapCount], ID: m.mapCount, NReduce: m.nReduce}
		m.mapCount++
		if m.mapCount == len(m.files) {
			m.currentPhase = reducePhase
		}
		return nil
	case reducePhase:
		reply.IsReduce = true
		reply.RTask = &ReduceTask{ID: m.reduceCount, NMap: len(m.files)}
		m.reduceCount++
		if m.reduceCount == m.nReduce {
			m.currentPhase = completed
		}
		return nil
	case completed:
		reply.IsDone = true
		return nil
	}
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
	if m.currentPhase == completed {
		return true
	}
	return false
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{files: files, nReduce: nReduce, currentPhase: mapPhase}

	// Your code here.

	m.server()
	return &m
}
