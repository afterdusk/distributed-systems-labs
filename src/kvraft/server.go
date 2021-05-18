package kvraft

import (
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

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Opcode string // "Put", "Append", "Get" TODO: Refactor
	Key    string
	Value  string
	Id     int64
	Seq    int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store            map[string]string
	responseCond     *sync.Cond
	lastAppliedIndex int
	lastAppliedOp    map[int64]Op
	readQueue        []int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	defer kv.responseCond.Broadcast()

	index, _, ok := kv.rf.Start(Op{Opcode: "Get", Key: args.Key, Id: args.Id, Seq: args.Seq})
	if !ok {
		reply.Err = "Not leader"
		return
	}

	kv.readQueue = append(kv.readQueue, index)
	for kv.lastAppliedIndex < index {
		kv.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		kv.mu.Lock()
		if _, isLeader := kv.rf.GetState(); !isLeader {
			kv.readQueue = kv.readQueue[1:]
			reply.Err = "Leadership lost"
			return
		}
	}

	if kv.lastAppliedOp[args.Id].Seq != args.Seq {
		DPrintf("index %v, kv.lastAppliedOp %v, args %v", index, kv.lastAppliedOp[args.Id], args)
		kv.readQueue = kv.readQueue[1:]
		reply.Err = "Leadership lost"
		return
	}

	reply.Value = kv.store[args.Key]
	kv.readQueue = kv.readQueue[1:]
	// DPrintf("[Server %v] Get Key %v, Index %v, LastApplied %v, ReadQueue: %v", kv.me, args.Key, index, kv.lastAppliedIndex, kv.readQueue)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	defer kv.responseCond.Broadcast()

	// TODO: Decouple PutAppendArgs.Op from Opcode
	index, _, ok := kv.rf.Start(Op{Opcode: args.Op, Key: args.Key, Value: args.Value, Id: args.Id, Seq: args.Seq})
	if !ok {
		reply.Err = "Failed"
		return
	}

	kv.readQueue = append(kv.readQueue, index)
	for kv.lastAppliedIndex < index {
		kv.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		kv.mu.Lock()
		if _, isLeader := kv.rf.GetState(); !isLeader {
			reply.Err = "Leadership lost"
			kv.readQueue = kv.readQueue[1:]
			return
		}
	}
	if kv.lastAppliedOp[args.Id].Seq != args.Seq {
		DPrintf("PutAppendArgs Key: %v, Value: %v, Op: %v, Seq: %v\n", args.Key, args.Value, args.Op, args.Seq)
		DPrintf("index %v, kv.lastAppliedOp %v\n", index, kv.lastAppliedOp[args.Id])
		reply.Err = "Leadership lost"
		kv.readQueue = kv.readQueue[1:]
		return
	}
	kv.readQueue = kv.readQueue[1:]
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
	kv.responseCond = sync.NewCond(&kv.mu)
	kv.lastAppliedOp = make(map[int64]Op)
	go kv.tick()

	return kv
}

func (kv *KVServer) tick() {
	for applyMsg := range kv.applyCh {
		kv.mu.Lock()

		for len(kv.readQueue) != 0 && kv.lastAppliedIndex == kv.readQueue[0] {
			kv.responseCond.Wait()
		}

		// TODO: Error handling on all levels including instructions
		if applyMsg.CommandValid {
			if op, ok := applyMsg.Command.(Op); ok {
				DPrintf("[kvserver %v] op: %v\n", kv.me, op)
				if kv.lastAppliedOp[op.Id].Seq != op.Seq {
					// DPrintf("[Server %v] Command %v Before - Key: %v, Op: %v, Store: %v", kv.rf.Me, applyMsg.CommandIndex, op.Key, op, kv.store)
					switch op.Opcode {
					case "Get":
						break
					case "Put":
						kv.store[op.Key] = op.Value
					case "Append":
						kv.store[op.Key] += op.Value
					}
					// DPrintf("[Server %v] Command %v After - Op: %v, Store: %v", kv.rf.Me, applyMsg.CommandIndex, op, kv.store)
				}
				kv.lastAppliedIndex = applyMsg.CommandIndex
				kv.lastAppliedOp[op.Id] = op
			}
		}

		kv.mu.Unlock()
	}
}
