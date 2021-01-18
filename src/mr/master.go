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

type taskPool struct {
	pending map[int]Task
	issued  map[int]Task
	mutex   sync.Mutex
}

func makeTaskPool() taskPool {
	return taskPool{pending: make(map[int]Task), issued: make(map[int]Task)}
}

func (tp *taskPool) insert(id int, task Task) {
	tp.mutex.Lock()
	defer tp.mutex.Unlock()
	tp.pending[id] = task
}

func (tp *taskPool) issue() (bool, Task) {
	tp.mutex.Lock()
	defer tp.mutex.Unlock()
	for id, task := range tp.pending {
		delete(tp.pending, id)
		tp.issued[id] = task
		time.AfterFunc(10*time.Second, func() { tp.reset(id) })
		return true, task
	}
	return false, nil
}

func (tp *taskPool) empty() bool {
	return len(tp.pending) == 0 && len(tp.issued) == 0
}

func (tp *taskPool) complete(id int) {
	tp.mutex.Lock()
	defer tp.mutex.Unlock()
	delete(tp.pending, id)
	delete(tp.issued, id)
}

func (tp *taskPool) reset(id int) bool {
	tp.mutex.Lock()
	defer tp.mutex.Unlock()
	if task, ok := tp.issued[id]; ok {
		delete(tp.issued, id)
		tp.pending[id] = task
		return true
	}
	return false
}

type Master struct {
	// Your definitions here.
	files       []string
	nReduce     int
	mapTasks    taskPool
	reduceTasks taskPool
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	for !m.mapTasks.empty() {
		if ok, task := m.mapTasks.issue(); ok {
			mapTask := task.(MapTask)
			reply.MTask = &mapTask
			return nil
		}
	}
	for !m.reduceTasks.empty() {
		if ok, task := m.reduceTasks.issue(); ok {
			reduceTask := task.(ReduceTask)
			reply.RTask = &reduceTask
			reply.IsReduce = true
			return nil
		}
	}
	reply.IsDone = true
	return nil
}

func (m *Master) PostCompletion(args *PostCompletionArgs, reply *PostCompletionReply) error {
	if args.IsReduce {
		m.reduceTasks.complete(args.RTask.ID)
		return nil
	}
	m.mapTasks.complete(args.MTask.ID)
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
	if m.mapTasks.empty() && m.reduceTasks.empty() {
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
	m := Master{files: files, nReduce: nReduce, reduceTasks: makeTaskPool(), mapTasks: makeTaskPool()}

	// Your code here.
	// Create map tasks
	for i := 0; i < len(files); i++ {
		m.mapTasks.insert(i, MapTask{Filename: files[i], ID: i, NReduce: nReduce})
	}
	// Create reduce tasks
	for i := 0; i < nReduce; i++ {
		m.reduceTasks.insert(i, ReduceTask{ID: i, NMap: len(files)})
	}

	m.server()
	return &m
}
