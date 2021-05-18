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

func (ck *Clerk) genRequestMeta() RequestMeta {
	return RequestMeta{
		ClientId:  ck.id,
		RequestId: nrand(),
	}
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
	args := &GetArgs{
		Key:         key,
		RequestMeta: ck.genRequestMeta(),
	}
	for {
		reply := &GetReply{}
		ok := ck.servers[ck.lastLeader].Call("KVServer.Get", args, reply)
		if !ok || reply.Err != OK {
			ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
			continue
		}
		return reply.Value
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
	args := &PutAppendArgs{
		Key:         key,
		Value:       value,
		Op:          op,
		RequestMeta: ck.genRequestMeta(),
	}
	for {
		reply := &PutAppendReply{}
		ok := ck.servers[ck.lastLeader].Call("KVServer.PutAppend", args, reply)
		if !ok || reply.Err != OK {
			ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
			continue
		}
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
