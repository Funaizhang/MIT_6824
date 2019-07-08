package shardkv

// import "shardmaster"
import (
	"bytes"
	"encoding/gob"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"shardmaster"
	"sync"
	"time"

	"github.com/sasha-s/go-deadlock"
)

const (
	Debug       = 0
	waitTimeout = 300
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
	OpType  string // "Get", "Put", "Append" or "Reconfig"
	Key     string
	Value   string
	Err     Err

	Config   shardmaster.Config
	LastOpID map[int64]int
	Store    [shardmaster.NShards]map[string]string
	Num      int   // for reconfiguring shards
	Shards   []int // for reconfiguring shards
}

type ShardKV struct {
	mu           deadlock.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	config   shardmaster.Config
	store    [shardmaster.NShards]map[string]string
	lastOpID map[int64]int              // map clerk to OpID, for deduplication
	msgCh    map[int]chan raft.ApplyMsg // map between CommandIndex to ch
	mck      *shardmaster.Clerk
}

// RPC handler: check if Op could be applied successfully
func (kv *ShardKV) ApplyOp(op1 Op) (bool, Op) {
	DPrintf("ApplyOp -- ShardKV[%d-%d]: op1 %+v", kv.me, kv.gid, op1)

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
		// DPrintf("ApplyOp -- ShardKV[%d-%d]: kv.msgCh[%d] receive op2 %+v; op1 %+v", kv.me, kv.gid, index, msg, op1)

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
			// DPrintf("ApplyOp -- ShardKV[%d-%d]: true", kv.me, kv.gid)
			return true, op2
		} else { // op2 is different from op1
			return false, op1
		}

	case <-time.After(waitTimeout * time.Millisecond):
		// DPrintf("ApplyOp -- ShardKV[%d-%d]: term[%d]", kv.me, kv.gid, term)

		kv.mu.Lock()
		delete(kv.msgCh, index)
		kv.mu.Unlock()

		return false, op1
	}
}

// RPC handler
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// DPrintf("Get -- ShardKV[%d-%d]", kv.me, kv.gid)

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
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// DPrintf("PutAppend -- ShardKV[%d-%d]", kv.me, kv.gid)

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

func (kv *ShardKV) Kill() {
	//
	// the tester calls Kill() when a ShardKV instance won't
	// be needed again. you are not required to do anything
	// in Kill(), but it might be convenient to (for example)
	// turn off debug output from this instance.
	//
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) isNewOp(op Op) bool {
	// check if op is new or duplicate
	currentOpID, ok := kv.lastOpID[op.ClerkID]
	if !ok || op.OpID > currentOpID {
		return true
	}
	// If it receives a command whose serial number has already been executed,
	// it responds immediately without re-executing the request.
	return false
}

func (kv *ShardKV) listenApplyCh() {
	// DPrintf("ShardKV[%d-%d]: listenApplyCh", kv.me, kv.gid)

	for {
		newApplyMsg := <-kv.applyCh

		if newApplyMsg.CommandValid { // received a command via applyCh
			op := newApplyMsg.Command.(Op)
			commandIndex := newApplyMsg.CommandIndex

			kv.mu.Lock()
			isNewOp := kv.isNewOp(op)

			if op.OpType != "Reconfig" && !kv.validKey(op.Key) { // check key belongs to this group
				op.Err = ErrWrongGroup
			} else { // key is valid for group
				switch op.OpType {
				case "Put":
					if isNewOp { // check duplicates
						DPrintf("ShardKV[%d-%d]: New Put %+v, kv.store %+v; kv.lastOpID %+v", kv.me, kv.gid, op, kv.store, kv.lastOpID)
						kv.store[key2shard(op.Key)][op.Key] = op.Value
						kv.lastOpID[op.ClerkID] = op.OpID // update kv.lastOpID
					}
					op.Err = OK
				case "Append":
					if isNewOp { // check duplicates
						DPrintf("ShardKV[%d-%d]: New Append %+v, kv.store %+v; kv.lastOpID %+v", kv.me, kv.gid, op, kv.store, kv.lastOpID)
						kv.store[key2shard(op.Key)][op.Key] += op.Value
						kv.lastOpID[op.ClerkID] = op.OpID // update kv.lastOpID
					}
					op.Err = OK
				case "Get":
					if isNewOp { // check duplicates
						kv.lastOpID[op.ClerkID] = op.OpID // update kv.lastOpID
					}
					val, ok := kv.store[key2shard(op.Key)][op.Key]
					DPrintf("ShardKV[%d-%d]: New Get %+v, kv.store %+v; kv.lastOpID %+v", kv.me, kv.gid, op, kv.store, kv.lastOpID)
					if ok {
						op.Value = val
						op.Err = OK
					} else {
						op.Err = ErrNoKey
					}
				case "Reconfig":
					// if isNewOp { // check duplicates
					// 	kv.lastOpID[op.ClerkID] = op.OpID // update kv.lastOpID
					// }

					DPrintf("ShardKV[%d-%d]: New Reconfig %+v, kv.store %+v; kv.lastOpID %+v", kv.me, kv.gid, op, kv.store, kv.lastOpID)

					if op.Config.Num < kv.config.Num {
						continue
					} else {
						// merge lastOpID of op and server
						for clerkID := range op.LastOpID {
							_, ok := kv.lastOpID[clerkID]
							if !ok || kv.lastOpID[clerkID] <= op.LastOpID[clerkID] {
								kv.lastOpID[clerkID] = op.LastOpID[clerkID]
							}
						}
						// merge store of op and server
						for shard, kvstore := range op.Store {
							for key, value := range kvstore {
								kv.store[shard][key] = value
							}
						}
						kv.config = op.Config
					}
					op.Err = OK

				default:
					log.Fatal("listenApplyCh -- invalid OpType")
				}
			}

			newApplyMsg.Command = op

			ch, ok := kv.msgCh[commandIndex]
			if ok {
				// DPrintf("ShardKV[%d-%d]: Notify RPC Op %+v", kv.me, kv.gid, newApplyMsg)
				ch <- newApplyMsg
			}

			// create snapshot if exceed maxraftstate
			if kv.maxraftstate != -1 && kv.rf.RaftStateSize() >= kv.maxraftstate {
				// DPrintf("ShardKV[%d-%d]: Creating snapchat at index[%d]", kv.me, kv.gid, commandIndex)
				w := new(bytes.Buffer)
				e := gob.NewEncoder(w)
				e.Encode(kv.store)
				e.Encode(kv.lastOpID)
				e.Encode(kv.config)
				data := w.Bytes()
				DPrintf("ShardKV[%d-%d]: Make snapshot kv.store %+v; kv.lastOpID %+v", kv.me, kv.gid, kv.store, kv.lastOpID)
				go kv.rf.MakeSnapshot(data, commandIndex)
			}
			kv.mu.Unlock()

		} else { // received a snapshot
			snapshot := newApplyMsg.Snapshot // snapshot is []data type
			r := bytes.NewBuffer(snapshot)
			d := labgob.NewDecoder(r)
			var lastIncludedIndex int
			var lastIncludedTerm int
			for i := 0; i < shardmaster.NShards; i++ {
				kv.store[i] = make(map[string]string)
			}
			kv.lastOpID = make(map[int64]int)

			kv.mu.Lock()
			d.Decode(&lastIncludedIndex)
			d.Decode(&lastIncludedTerm)
			d.Decode(&kv.store)
			d.Decode(&kv.lastOpID)
			d.Decode(&kv.config)
			DPrintf("ShardKV[%d-%d]: Applied a snapshot; lastIncludedIndex[%d], lastIncludedTerm[%d]; kv.store %+v", kv.me, kv.gid, lastIncludedIndex, lastIncludedTerm, kv.store)
			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) listenConfig() {
	// The biggest modification will be to have your server detect when a configuration happens
	// and start accepting requests whose keys match shards that it now owns.

	DPrintf("ShardKV[%d-%d]: start listenConfig", kv.me, kv.gid)

	for {
		_, isLeader := kv.rf.GetState()
		if isLeader {
			// DPrintf("ShardKV[%d-%d] is leader", kv.me, kv.gid)
			latestConfig := kv.mck.Query(-1) // get latest configuration
			DPrintf("ShardKV[%d-%d] latestConfig %+v", kv.me, kv.gid, latestConfig)
			myConfig := kv.config
			DPrintf("ShardKV[%d-%d] myConfig %+v", kv.me, kv.gid, myConfig)

			if latestConfig.Num > myConfig.Num { // check the queried config is not outdated
				for i := myConfig.Num + 1; i <= latestConfig.Num; i++ { // process config one at a time, in order
					newConfig := kv.mck.Query(i)
					ok := true
					var wg sync.WaitGroup

					op := Op{
						OpType:   "Reconfig",
						Config:   newConfig,
						LastOpID: make(map[int64]int),
					}
					for i := 0; i < shardmaster.NShards; i++ {
						op.Store[i] = make(map[string]string)
					}

					// find out shards to be moved
					pullShards := make(map[int][]int)
					for i := 0; i < shardmaster.NShards; i++ {
						oldgid := myConfig.Shards[i]
						if kv.gid == newConfig.Shards[i] && kv.gid != oldgid && oldgid != 0 { // shard changed group
							_, ok := pullShards[oldgid]
							if !ok {
								pullShards[oldgid] = make([]int, 0)
							}
							pullShards[oldgid] = append(pullShards[oldgid], i)
						}
					}

					DPrintf("ShardKV[%d-%d] pullShards %+v", kv.me, kv.gid, pullShards)

					for gid, shards := range pullShards {
						wg.Add(1)

						shardOp := Op{
							Num:    newConfig.Num,
							Shards: shards,
						}

						go func(gid int, args Op) {
							okPull, reply := kv.doPullShard(gid, &args)
							if okPull {
								DPrintf("ShardKV[%d-%d] okPull %+v; reply %+v", kv.me, kv.gid, okPull, reply)
								// merge LastOpID map
								for clerkID := range args.LastOpID {
									_, ok := op.LastOpID[clerkID]
									if !ok || op.LastOpID[clerkID] < reply.LastOpID[clerkID] {
										op.LastOpID[clerkID] = reply.LastOpID[clerkID]
									}
								}
								// merge Store
								for _, shard := range args.Shards {
									kvstore := reply.Store[shard]
									for key, value := range kvstore {
										op.Store[shard][key] = value
									}
								}
								DPrintf("ShardKV[%d-%d] merged op %+v", kv.me, kv.gid, op)
							} else {
								ok = false
							}
							wg.Done()
						}(gid, shardOp)
					}
					wg.Wait()

					if !ok {
						break
					}

					DPrintf("ShardKV[%d-%d] applying Reconfig op %+v", kv.me, kv.gid, op)

					// check if Op can be successfully applied
					okApply, _ := kv.ApplyOp(op)
					if !okApply { // fail to ApplyOp
						break
					} else { // success
						continue
					}
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) doPullShard(gid int, op *Op) (bool, *Op) {
	reply := &Op{}
	for _, server := range kv.config.Groups[gid] {
		srv := kv.make_end(server)
		ok := srv.Call("ShardKV.PullShard", op, reply)
		if ok {
			DPrintf("ShardKV[%d-%d] RPC call ok %+v; reply %+v", kv.me, kv.gid, ok, reply)
			if op.Err == OK {
				return true, reply
			} else if op.Err == ErrWrongGroup {
				return false, reply
			}
		}
	}
	return true, reply
}

func (kv *ShardKV) PullShard(op *Op, reply *Op) {
	DPrintf("ShardKV[%d-%d] before kv.PullShard reply %+v", kv.me, kv.gid, reply)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// copy Store and LastOpID
	reply.LastOpID = make(map[int64]int)
	for clerkID, opID := range kv.lastOpID {
		reply.LastOpID[clerkID] = opID
	}
	for i := 0; i < shardmaster.NShards; i++ {
		reply.Store[i] = make(map[string]string)
	}
	for _, shard := range op.Shards {
		for key, value := range kv.store[shard] {
			reply.Store[shard][key] = value
		}
	}
	reply.Err = OK
	DPrintf("ShardKV[%d-%d] after kv.PullShard reply %+v", kv.me, kv.gid, reply)
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(shardmaster.Config{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg, 1000)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	for i := 0; i < shardmaster.NShards; i++ {
		kv.store[i] = make(map[string]string)
	}
	kv.lastOpID = make(map[int64]int)
	kv.msgCh = make(map[int]chan raft.ApplyMsg)
	kv.mck = shardmaster.MakeClerk(kv.masters)

	go kv.listenConfig()
	go kv.listenApplyCh()

	return kv
}

func (kv *ShardKV) validKey(key string) bool {
	// check if key is in replica group
	gid := kv.config.Shards[key2shard(key)]
	check := (gid == kv.gid)
	return check
}
