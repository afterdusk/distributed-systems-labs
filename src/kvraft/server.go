package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type opcode int

const (
	getOp opcode = iota
	putOp
	appendOp
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Code  opcode
	Key   string
	Value string
	RequestMeta
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store             map[string]string // kv store
	tickCond          *sync.Cond        // condition variable for tick
	requestCond       *sync.Cond        // condition variable for requests
	lastAppliedIndex  int               // last log index applied on state machine
	lastAppliedOp     map[int64]Op      // last operation applied to each client
	requestIndexQueue []int             // log indices of pending requests
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	defer kv.tickCond.Broadcast()

	index, _, ok := kv.rf.Start(Op{
		Code:        getOp,
		Key:         args.Key,
		RequestMeta: args.RequestMeta,
	})
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}

	kv.requestIndexQueue = append(kv.requestIndexQueue, index)
	defer func() { kv.requestIndexQueue = kv.requestIndexQueue[1:] }()
	startTime := time.Now()

	for kv.lastAppliedIndex < index {
		kv.requestCond.Wait()
		if _, isLeader := kv.rf.GetState(); !isLeader || kv.killed() || startTime.Add(time.Second).Before(time.Now()) {
			reply.Err = ErrWrongLeader
			return
		}
	}

	if kv.lastAppliedOp[args.ClientId].RequestId != args.RequestId {
		reply.Err = ErrWrongLeader
		return
	}

	reply.Err = OK
	reply.Value = kv.store[args.Key]
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	defer kv.tickCond.Broadcast()

	code := putOp
	if args.Op == "Append" {
		code = appendOp
	}

	index, _, ok := kv.rf.Start(Op{
		Code:        code,
		Key:         args.Key,
		Value:       args.Value,
		RequestMeta: args.RequestMeta,
	})
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}

	kv.requestIndexQueue = append(kv.requestIndexQueue, index)
	defer func() { kv.requestIndexQueue = kv.requestIndexQueue[1:] }()
	startTime := time.Now()

	for kv.lastAppliedIndex < index {
		kv.requestCond.Wait()
		if _, isLeader := kv.rf.GetState(); !isLeader || kv.killed() || startTime.Add(time.Second).Before(time.Now()) {
			reply.Err = ErrWrongLeader
			return
		}
	}

	if kv.lastAppliedOp[args.ClientId].RequestId != args.RequestId {
		reply.Err = ErrWrongLeader
		return
	}

	reply.Err = OK
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.store = make(map[string]string)
	kv.tickCond = sync.NewCond(&kv.mu)
	kv.requestCond = sync.NewCond(&kv.mu)
	kv.lastAppliedOp = make(map[int64]Op)
	go kv.tick()

	return kv
}

func (kv *KVServer) tick() {
	for !kv.killed() {
		select {
		case applyMsg := <-kv.applyCh:
			kv.mu.Lock()

			for len(kv.requestIndexQueue) != 0 && kv.lastAppliedIndex == kv.requestIndexQueue[0] {
				kv.tickCond.Wait()
			}

			if applyMsg.CommandValid {
				if op, ok := applyMsg.Command.(Op); ok {
					if kv.lastAppliedOp[op.ClientId].RequestId != op.RequestId {
						switch op.Code {
						case getOp:
							break
						case putOp:
							kv.store[op.Key] = op.Value
						case appendOp:
							kv.store[op.Key] += op.Value
						}
					}
					kv.lastAppliedIndex = applyMsg.CommandIndex
					kv.lastAppliedOp[op.ClientId] = op
					kv.requestCond.Broadcast()
				}
			} else if applyMsg.SnapshotValid {
				buf := bytes.NewBuffer(applyMsg.Snapshot)
				dec := labgob.NewDecoder(buf)
				if err := dec.Decode(&kv.store); err != nil {
					log.Fatal(err)
				}
				if err := dec.Decode(&kv.lastAppliedOp); err != nil {
					log.Fatal(err)
				}
				kv.rf.CondInstallSnapshot(applyMsg.SnapshotTerm, applyMsg.SnapshotIndex, applyMsg.Snapshot)
				kv.lastAppliedIndex = applyMsg.SnapshotIndex
			}
			kv.mu.Unlock()
		default:
			if kv.maxraftstate != -1 && kv.rf.Size() > kv.maxraftstate {
				buf := new(bytes.Buffer)
				enc := labgob.NewEncoder(buf)
				if err := enc.Encode(kv.store); err != nil {
					log.Fatal(err)
				}
				if err := enc.Encode(kv.lastAppliedOp); err != nil {
					log.Fatal(err)
				}
				kv.rf.Snapshot(kv.lastAppliedIndex, buf.Bytes())
			}
			kv.requestCond.Broadcast()
		}
	}
}
