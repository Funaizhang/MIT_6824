package shardmaster

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"time"

	"github.com/sasha-s/go-deadlock"
)

const (
	Debug       = 0
	waitTimeout = 400
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	OpID     int
	ClientID int64
	OpType   string // "Join", "Leave", "Move" or "Query"
	Err      Err

	Servers map[int][]string // Join
	GIDs    []int            // Leave
	Shard   int              // Move
	GID     int              // Move
	Num     int              // Query
	Config  Config           // Query
}

type ShardMaster struct {
	mu      deadlock.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	// maxraftstate int // snapshot if log grows this big

	configs  []Config                   // indexed by config num
	lastOpID map[int64]int              // map clerk to OpID, for deduplication
	msgCh    map[int]chan raft.ApplyMsg // map between CommandIndex to ch
}

// RPC handler: check if Op could be applied successfully
func (sm *ShardMaster) ApplyOp(op1 Op) (bool, Op) {
	// DPrintf("ApplyOp -- ShardMaster[%d]", sm.me)

	// check if I'm leader
	index, term, isLeader := sm.rf.Start(op1)
	if !isLeader {
		return false, op1
	}

	sm.mu.Lock()
	// map index to a channel
	sm.msgCh[index] = make(chan raft.ApplyMsg, 1)
	ch, _ := sm.msgCh[index]
	sm.mu.Unlock()

	select {
	case msg := <-ch:
		DPrintf("ApplyOp -- ShardMaster[%d]: sm.msgCh[%d] receive op2 %+v; op1 %+v", sm.me, index, msg, op1)

		sm.mu.Lock()
		delete(sm.msgCh, index)
		sm.mu.Unlock()

		op2 := msg.Command.(Op)
		commandIndex := msg.CommandIndex
		commandTerm := msg.CommandTerm

		// One way to do this is for the server to detect that it has lost leadership,
		// by noticing that a different request has appeared at the index returned by Start(),
		// or that Raft's term has changed.
		if commandIndex == index && commandTerm == term && op1.OpID == op2.OpID && op1.ClientID == op2.ClientID { // same op
			DPrintf("ApplyOp -- ShardMaster[%d]: true", sm.me)
			return true, op2
		} else { // op2 is different from op1
			return false, op1
		}

	case <-time.After(waitTimeout * time.Millisecond):
		DPrintf("ApplyOp -- ShardMaster[%d]: term[%d] timeout", sm.me, term)

		sm.mu.Lock()
		delete(sm.msgCh, index)
		sm.mu.Unlock()

		return false, op1
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op1 := Op{
		OpID:     args.OpID,
		ClientID: args.ClientID,
		OpType:   "Join",
		Servers:  args.Servers,
	}

	// check if Op can be successfully applied
	okApply, op2 := sm.ApplyOp(op1)

	if !okApply {
		reply.WrongLeader = true
	} else { // success
		reply.WrongLeader = false
		reply.Err = op2.Err
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op1 := Op{
		OpID:     args.OpID,
		ClientID: args.ClientID,
		OpType:   "Leave",
		GIDs:     args.GIDs,
	}

	// check if Op can be successfully applied
	okApply, op2 := sm.ApplyOp(op1)

	if !okApply {
		reply.WrongLeader = true
	} else { // success
		reply.WrongLeader = false
		reply.Err = op2.Err
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op1 := Op{
		OpID:     args.OpID,
		ClientID: args.ClientID,
		OpType:   "Move",
		Shard:    args.Shard,
		GID:      args.GID,
	}

	// check if Op can be successfully applied
	okApply, op2 := sm.ApplyOp(op1)

	if !okApply {
		reply.WrongLeader = true
	} else { // success
		reply.WrongLeader = false
		reply.Err = op2.Err
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op1 := Op{
		OpID:     args.OpID,
		ClientID: args.ClientID,
		OpType:   "Query",
		Num:      args.Num,
	}

	// check if Op can be successfully applied
	okApply, op2 := sm.ApplyOp(op1)

	if !okApply {
		reply.WrongLeader = true
	} else { // success
		reply.WrongLeader = false
		reply.Err = op2.Err
		reply.Config = op2.Config
	}
}

func (sm *ShardMaster) Kill() {
	//
	// the tester calls Kill() when a ShardMaster instance won't
	// be needed again. you are not required to do anything
	// in Kill(), but it might be convenient to (for example)
	// turn off debug output from this instance.
	//
	sm.rf.Kill()
	// Your code here, if desired.
}

func (sm *ShardMaster) isNewOp(op Op) bool {
	// check if op is new or duplicate
	currentOpID, ok := sm.lastOpID[op.ClientID]
	if !ok || op.OpID > currentOpID {
		return true
	}
	// If it receives a command whose serial number has already been executed,
	// it responds immediately without re-executing the request.
	return false
}

func (sm *ShardMaster) listenApplyCh() {
	DPrintf("ShardMaster[%d]: listenApplyCh", sm.me)

	for {
		newApplyMsg := <-sm.applyCh

		if newApplyMsg.CommandValid { // received a command via applyCh
			op := newApplyMsg.Command.(Op)
			commandIndex := newApplyMsg.CommandIndex

			sm.mu.Lock()
			isNewOp := sm.isNewOp(op)

			switch op.OpType {
			case "Join":
				if isNewOp { // check duplicates
					DPrintf("ShardMaster[%d]: New Join %+v", sm.me, op)
					config := sm.makeConfig(-1, false)
					for gid, servers := range op.Servers {
						config.Groups[gid] = servers
					}
					sm.rebalance(&config)
					DPrintf("ShardMaster[%d]: Rebalancing Join opID[%d] config %+v", sm.me, op.OpID, config)
					sm.configs = append(sm.configs, config)
				}
				op.Err = OK

			case "Leave":
				if isNewOp { // check duplicates
					DPrintf("ShardMaster[%d]: New Leave %+v", sm.me, op)
					config := sm.makeConfig(-1, false)
					for _, gid := range op.GIDs { // op.GIDs are the Leaving groups
						delete(config.Groups, gid)
					}
					sm.rebalance(&config)
					DPrintf("ShardMaster[%d]: Rebalancing Leave opID[%d] config %+v", sm.me, op.OpID, config)
					sm.configs = append(sm.configs, config)
				}
				op.Err = OK

			case "Move":
				if isNewOp { // check duplicates
					DPrintf("ShardMaster[%d]: New Move %+v", sm.me, op)
					config := sm.makeConfig(-1, false)
					config.Shards[op.Shard] = op.GID
					sm.configs = append(sm.configs, config)
				}
				op.Err = OK

			case "Query":
				if isNewOp { // check duplicates
					DPrintf("ShardMaster[%d]: New Query %+v", sm.me, op)
					op.Config = sm.makeConfig(op.Num, true)
				}
				op.Err = OK

			// case "Get" || "Put" || "Append":
			default:
				log.Fatal("listenApplyCh -- invalid OpType")
			}

			// update sm.lastOpID
			sm.lastOpID[op.ClientID] = op.OpID

			newApplyMsg.Command = op

			ch, ok := sm.msgCh[commandIndex]
			if ok {
				DPrintf("ShardMaster[%d]: Notify RPC Op %+v", sm.me, newApplyMsg)
				ch <- newApplyMsg
			}

			// // create snapshot if exceed maxraftstate
			// if sm.maxraftstate != -1 && sm.rf.RaftStateSize() >= sm.maxraftstate {
			// 	DPrintf("ShardMaster[%d]: Creating snapchat at index[%d]", sm.me, commandIndex)
			// 	w := new(bytes.Buffer)
			// 	e := gob.NewEncoder(w)
			// 	e.Encode(sm.store)
			// 	e.Encode(sm.lastOpID)
			// 	data := w.Bytes()
			// 	go sm.rf.MakeSnapshot(data, commandIndex)
			// }
			sm.mu.Unlock()

		} else { // received a snapshot
			// snapshot := newApplyMsg.Snapshot // snapshot is []data type
			// r := bytes.NewBuffer(snapshot)
			// d := labgob.NewDecoder(r)
			// var lastIncludedIndex int
			// var lastIncludedTerm int
			// sm.configs = make([]Config)
			// sm.lastOpID = make(map[int64]int)

			// sm.mu.Lock()
			// if d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
			// 	log.Fatal("INVESTIGATE: listenApplyCh")
			// } else {
			// 	d.Decode(&lastIncludedIndex)
			// 	d.Decode(&lastIncludedTerm)
			// 	d.Decode(&sm.configs)
			// 	d.Decode(&sm.lastOpID)
			// }
			// DPrintf("ShardMaster[%d]: Applied a snapshot; lastIncludedIndex[%d], lastIncludedTerm[%d]", sm.me, lastIncludedIndex, lastIncludedTerm)
			// sm.mu.Unlock()
		}
	}
}

func (sm *ShardMaster) makeConfig(num int, query bool) Config {
	// helper function to create a copy of the desired config
	var oldConfig Config
	if num == -1 || num >= len(sm.configs) {
		oldConfig = sm.configs[len(sm.configs)-1]
	} else {
		oldConfig = sm.configs[num]
	}

	// Hint: Go maps are references. Thus if you want to create a new Config based on a previous one,
	// you need to create a new map object (with make()) and copy the keys and values individually.
	// newGroup := map[int][]string{}
	newConfig := Config{
		Num:    oldConfig.Num,
		Shards: oldConfig.Shards,
		Groups: map[int][]string{},
	}
	if !query {
		newConfig.Num++
	}
	for gid, servers := range oldConfig.Groups {
		newConfig.Groups[gid] = servers
	}
	return newConfig
}

func (sm *ShardMaster) rebalance(config *Config) {
	NGroups := len(config.Groups)
	// if no group exists
	if NGroups == 0 {
		for i := 0; i < len(config.Shards); i++ {
			config.Shards[i] = 0
		}
		return
	}

	GIDtoShard := make(map[int][]int) // gid -> shard
	// init GIDtoShard
	for gid := range config.Groups {
		GIDtoShard[gid] = make([]int, 0)
	}
	// move any shard without group to group 0
	for shard := 0; shard < len(config.Shards); shard++ {
		gid := config.Shards[shard]
		_, ok := config.Groups[gid]
		if ok { // shard has been assigned a group
			GIDtoShard[gid] = append(GIDtoShard[gid], shard)
		} else { // shard has no assigned group, probably deleted
			GIDtoShard[0] = append(GIDtoShard[0], shard)
		}
	}

	mean := NShards / NGroups

	// if a group contains >mean number of shards, move the exceeded shards to group 0
	for gid, shards := range GIDtoShard {
		if gid == 0 {
			continue
		} else {
			if len(shards) > mean {
				GIDtoShard[0] = append(GIDtoShard[0], GIDtoShard[gid][mean:]...)
				GIDtoShard[gid] = GIDtoShard[gid][:mean]
			}
		}
	}
	DPrintf("ShardMaster[%d]: GIDtoShard %+v", sm.me, GIDtoShard)

	// move every shard in group 0 to other groups
	for _, shard := range GIDtoShard[0] {
		// find the group with fewest shards
		minNShards := NShards + 1
		minGroup := 0
		for gid, shards := range GIDtoShard {
			if gid != 0 && len(shards) < minNShards {
				minNShards = len(shards)
				minGroup = gid
			}
		}

		// append shard to the group with fewest shards
		GIDtoShard[minGroup] = append(GIDtoShard[minGroup], shard)
		// change config.Shards
		config.Shards[shard] = minGroup
	}

	// clear group 0
	delete(GIDtoShard, 0)
	DPrintf("ShardMaster[%d]: Rebalanced GIDtoShard %+v", sm.me, GIDtoShard)
}

func (sm *ShardMaster) Raft() *raft.Raft {
	// needed by shardkv tester
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.lastOpID = make(map[int64]int)
	sm.msgCh = make(map[int]chan raft.ApplyMsg)

	go sm.listenApplyCh()
	return sm
}
