package raftkv

import (
	"bytes"
	"encoding/gob"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"time"

	"github.com/sasha-s/go-deadlock"
)

const (
	Debug       = 0
	waitTimeout = 800
)

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
	OpID    int
	ClerkID int64
	OpType  string // "Get", "Put" or "Append"
	Key     string
	Value   string
	Err     Err
}

type KVServer struct {
	mu      deadlock.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	// persister *raft.Raft.persister

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store    map[string]string
	lastOpID map[int64]int              // map clerk to OpID, for deduplication
	msgCh    map[int]chan raft.ApplyMsg // map between CommandIndex to ch

	// persister *raft.Persister //where is this used???
}

// RPC handler: check if Op could be applied successfully
func (kv *KVServer) ApplyOp(op1 Op) (bool, Op) {
	// DPrintf("ApplyOp -- KVServer[%d]", kv.me)

	// check if I'm leader
	index, term, isLeader := kv.rf.Start(op1)
	if !isLeader {
		return false, op1
	}

	kv.mu.Lock()
	// map index to a channel
	kv.msgCh[index] = make(chan raft.ApplyMsg, 1)
	ch, _ := kv.msgCh[index]
	kv.mu.Unlock()

	select {
	case msg := <-ch:
		// DPrintf("ApplyOp -- KVServer[%d]: kv.msgCh[%d] receive op2 %+v; op1 %+v", kv.me, index, msg, op1)

		kv.mu.Lock()
		delete(kv.msgCh, index)
		kv.mu.Unlock()

		op2 := msg.Command.(Op)
		commandIndex := msg.CommandIndex
		commandTerm := msg.CommandTerm

		// One way to do this is for the server to detect that it has lost leadership,
		// by noticing that a different request has appeared at the index returned by Start(),
		// or that Raft's term has changed.
		if commandIndex == index && commandTerm == term && op1.OpID == op2.OpID && op1.ClerkID == op2.ClerkID { // same op
			// DPrintf("ApplyOp -- KVServer[%d]: true", kv.me)
			return true, op2
		} else { // op2 is different from op1
			return false, op1
		}

	case <-time.After(waitTimeout * time.Millisecond):
		// DPrintf("ApplyOp -- KVServer[%d]: term[%d]", kv.me, term)

		kv.mu.Lock()
		delete(kv.msgCh, index)
		kv.mu.Unlock()

		return false, op1
	}
}

// RPC handler
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// DPrintf("Get -- KVServer[%d]", kv.me)

	op1 := Op{
		OpID:    args.OpID,
		ClerkID: args.ClerkID,
		OpType:  "Get",
		Key:     args.Key,
	}

	// check if Op can be successfully applied
	okApply, op2 := kv.ApplyOp(op1)

	if !okApply {
		reply.WrongLeader = true
	} else { // success
		reply.WrongLeader = false
		reply.Err = op2.Err
		reply.Value = op2.Value
	}
}

// RPC handler
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// DPrintf("PutAppend -- KVServer[%d]", kv.me)

	op1 := Op{
		OpID:    args.OpID,
		ClerkID: args.ClerkID,
		OpType:  args.OpType,
		Key:     args.Key,
		Value:   args.Value,
	}

	// check if Op can be successfully applied
	okApply, op2 := kv.ApplyOp(op1)

	if !okApply {
		reply.WrongLeader = true
	} else { // success
		reply.WrongLeader = false
		reply.Err = op2.Err
	}
}

func (kv *KVServer) Kill() {
	//
	// the tester calls Kill() when a KVServer instance won't
	// be needed again. you are not required to do anything
	// in Kill(), but it might be convenient to (for example)
	// turn off debug output from this instance.
	//
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) isNewOp(op Op) bool {
	// check if op is new or duplicate
	currentOpID, ok := kv.lastOpID[op.ClerkID]
	if !ok || op.OpID > currentOpID {
		return true
	}
	// If it receives a command whose serial number has already been executed,
	// it responds immediately without re-executing the request.
	return false
}

func (kv *KVServer) listenApplyCh() {
	// DPrintf("KVServer[%d]: listenApplyCh", kv.me)

	for {
		newApplyMsg := <-kv.applyCh

		if newApplyMsg.CommandValid { // received a command via applyCh
			op := newApplyMsg.Command.(Op)
			commandIndex := newApplyMsg.CommandIndex

			kv.mu.Lock()
			isNewOp := kv.isNewOp(op)

			switch op.OpType {
			case "Put":
				if isNewOp { // check duplicates
					DPrintf("KVServer[%d]: New Put %+v, kv.store %+v; kv.lastOpID %+v", kv.me, op, kv.store, kv.lastOpID)
					kv.store[op.Key] = op.Value
					kv.lastOpID[op.ClerkID] = op.OpID // update kv.lastOpID
				}
				op.Err = OK
			case "Append":
				if isNewOp { // check duplicates
					DPrintf("KVServer[%d]: New Append %+v, kv.store %+v; kv.lastOpID %+v", kv.me, op, kv.store, kv.lastOpID)
					kv.store[op.Key] += op.Value
					kv.lastOpID[op.ClerkID] = op.OpID // update kv.lastOpID
				}
				op.Err = OK
			case "Get":
				if isNewOp { // check duplicates
					kv.lastOpID[op.ClerkID] = op.OpID // update kv.lastOpID
				}
				val, ok := kv.store[op.Key]
				DPrintf("KVServer[%d]: New Get %+v, kv.store %+v; kv.lastOpID %+v", kv.me, op, kv.store, kv.lastOpID)
				if ok {
					op.Value = val
					op.Err = OK
				} else {
					op.Err = ErrNoKey
				}
			default:
				log.Fatal("listenApplyCh -- not Get, Put or Append")
			}

			newApplyMsg.Command = op

			ch, ok := kv.msgCh[commandIndex]
			if ok {
				// DPrintf("KVServer[%d]: Notify RPC Op %+v", kv.me, newApplyMsg)
				ch <- newApplyMsg
			}

			// create snapshot if exceed maxraftstate
			if kv.maxraftstate != -1 && kv.rf.RaftStateSize() >= kv.maxraftstate {
				// DPrintf("KVServer[%d]: Creating snapchat at index[%d]", kv.me, commandIndex)
				w := new(bytes.Buffer)
				e := gob.NewEncoder(w)
				e.Encode(kv.store)
				e.Encode(kv.lastOpID)
				data := w.Bytes()
				DPrintf("KVServer[%d]: Make snapshot kv.store %+v; kv.lastOpID %+v", kv.me, kv.store, kv.lastOpID)
				go kv.rf.MakeSnapshot(data, commandIndex)
			}
			kv.mu.Unlock()

		} else { // received a snapshot
			snapshot := newApplyMsg.Snapshot // snapshot is []data type
			r := bytes.NewBuffer(snapshot)
			d := labgob.NewDecoder(r)
			var lastIncludedIndex int
			var lastIncludedTerm int
			kv.store = make(map[string]string)
			kv.lastOpID = make(map[int64]int)

			kv.mu.Lock()
			// if d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
			// 	log.Fatal("INVESTIGATE: listenApplyCh")
			// } else {
			d.Decode(&lastIncludedIndex)
			d.Decode(&lastIncludedTerm)
			d.Decode(&kv.store)
			d.Decode(&kv.lastOpID)
			// }
			DPrintf("KVServer[%d]: Applied a snapshot; lastIncludedIndex[%d], lastIncludedTerm[%d]; kv.store %+v", kv.me, lastIncludedIndex, lastIncludedTerm, kv.store)
			kv.mu.Unlock()
		}
	}
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
	kv.applyCh = make(chan raft.ApplyMsg, 1000)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.store = make(map[string]string)
	kv.lastOpID = make(map[int64]int)
	kv.msgCh = make(map[int]chan raft.ApplyMsg)

	go kv.listenApplyCh()

	return kv
}
