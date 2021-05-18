package kvraft

import (
	"crypto/rand"
	"math/big"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id         int64
	lastLeader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.id = nrand()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// assume no collision with nrand()
	args := &GetArgs{
		Key: key,
		Id:  ck.id,
		Seq: nrand(),
	}
	DPrintf("[Client %v][Seq %v] Get: Key %v", ck.id, args.Seq, key)
	for {
		for i := 0; i < len(ck.servers); i++ {
			// for i, server := range ck.servers {
			reply := &GetReply{}
			ok := ck.servers[(ck.lastLeader+i)%len(ck.servers)].Call("KVServer.Get", args, reply)
			if !ok || reply.Err != "" {
				continue
			}
			ck.lastLeader = (ck.lastLeader + i) % len(ck.servers)
			DPrintf("[Client %v][Seq %v] Get Success Key %v, Value %v", ck.id, args.Seq, key, reply.Value)
			return reply.Value
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// assume no collision with nrand()
	args := &PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		Id:    ck.id,
		Seq:   nrand(),
	}
	DPrintf("[Client %v][Seq %v] PutAppend: Key %v Value %v", ck.id, args.Seq, key, value)
	for {
		for i := 0; i < len(ck.servers); i++ {
			leader := (ck.lastLeader + i) % len(ck.servers)

			reply := &PutAppendReply{}
			ok := ck.servers[leader].Call("KVServer.PutAppend", args, reply)
			if !ok || reply.Err != "" {
				// DPrintf("[Client %v] PutAppend Error with server %v (ok %v, err %v) Key %v, Value %v, Op %v\n", ck.id, i, ok, reply.Err, key, value, op)
				continue
			}
			DPrintf("[Client %v][Seq %v] PutAppend Success with server %v Key %v, Value %v, Op %v\n", ck.id, args.Seq, i, key, value, op)
			ck.lastLeader = leader
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
