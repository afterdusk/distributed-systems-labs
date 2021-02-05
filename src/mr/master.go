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
	pending map[int]interface{}
	issued  map[int]interface{}
	mutex   sync.Mutex
}

func makeTaskPool() taskPool {
	return taskPool{pending: make(map[int]interface{}), issued: make(map[int]interface{})}
}

func (tp *taskPool) insert(id int, task interface{}) {
	tp.mutex.Lock()
	defer tp.mutex.Unlock()
	tp.pending[id] = task
}

func (tp *taskPool) issue() (bool, interface{}) {
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
			mapTask, castOk := task.(MapTask)
			if !castOk {
				log.Fatal("did not receive map task from map task pool")
			}
			reply.MTask = &mapTask
			return nil
		}
	}
	for !m.reduceTasks.empty() {
		if ok, task := m.reduceTasks.issue(); ok {
			reduceTask, castOk := task.(ReduceTask)
			if !castOk {
				log.Fatal("did not receive reduce task from reduce task pool")
			}
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
		if args.RTask == nil {
			log.Fatal("received nil reduce task")
		}
		m.reduceTasks.complete(args.RTask.ID)
	} else {
		if args.MTask == nil {
			log.Fatal("received nil map task")
		}
		m.mapTasks.complete(args.MTask.ID)
	}
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
	return m.mapTasks.empty() && m.reduceTasks.empty()
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
