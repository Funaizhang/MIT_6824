package raftkv

import (
	"crypto/rand"
	"labrpc"
	"math/big"
	"time"

	"github.com/sasha-s/go-deadlock"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu      deadlock.Mutex
	clerkID int64
	opID    int
	leader  int64
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
	ck.clerkID = nrand()
	ck.opID = 0
	ck.leader = 0
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
	// You will have to modify this function.

	args := &GetArgs{}
	args.Key = key
	ck.mu.Lock()
	args.ClerkID = ck.clerkID
	args.OpID = ck.opID
	ck.opID++
	ck.mu.Unlock()

	for { // keep trying till client finds the right leader
		// try each known server.
		for _, srv := range ck.servers {
			var reply GetReply
			ok := srv.Call("KVServer.Get", args, &reply)
			if ok && reply.WrongLeader == false { // correct leader
				return reply.Value
			}
		}
		time.Sleep(100 * time.Millisecond)
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
	// You will have to modify this function.

	args := &PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.OpType = op // "Put" or "Append"
	ck.mu.Lock()
	args.ClerkID = ck.clerkID
	args.OpID = ck.opID
	ck.opID++
	ck.mu.Unlock()

	for { // keep trying till client finds the right leader
		// try each known server.
		for _, srv := range ck.servers {
			var reply PutAppendReply
			ok := srv.Call("KVServer.PutAppend", args, &reply)
			if ok && reply.WrongLeader == false { // correct leader
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
