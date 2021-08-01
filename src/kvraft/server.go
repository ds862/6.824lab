package kvraft

import (
	"../labgob"
	"../labrpc"
	"log"
	"../raft"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// Op是要发向RAFT的COMMAND
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters, 字段名必须以大写字母开头，
	// otherwise RPC will break.
	OpType string "operation type(eg. put/append)"
    Key    string
    Value  string
	// 注意，这里也要加id和请求序列号
	Cid     int64
	SeqNum  int
}

// KV SERVER 自己会有把锁，有一个RAFT peer，自己的序号，还有一个和RAFT通信的APPLY CHANNEL
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db      map[string]string  // 键值对，用作kvDb
	chMap   map[int]chan Op    // command在log中的index(键)和Op channel(值)的映射
	cid2Seq map[int64]int      // 客户端id及对应的请求序列
	killCh  chan bool          // 实现Kill
}

// 和CLERK 通信的 RPC HANDLER
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// client发来的command
	originOp := Op{"Get", args.Key, "",0,0}
	reply.WrongLeader = true
	// 向Raft发送消息
	// _,isLeader := kv.rf.GetState()的判断被包含在index,_,isLeader := kv.rf.Start(originOp)中
	// 在GET HANDLER里发送这个命令，拿到INDEX，随后去监听INDEX直到消息回来。然后去KV.DB里读，还需要CHECK OP是不是一致，不一致的话，就代表有错。
	index, _, isLeader := kv.rf.Start(originOp)
	if !isLeader {return}
	ch := kv.putIfAbsent(index)
	// 检测StartKVServer()中goroutine发来的chMap[idx]（index对应的Op数据）
	// 仅使用 op := <- ch 会阻塞，因此需要实现timeout逻辑
	op := beNotified(ch)
	// 如果两个Op相同，则成功回复reply.Value = kv.db[op.Key]
	if equalOp(op,originOp) {
		reply.WrongLeader = false
		kv.mu.Lock()
		reply.Value = kv.db[op.Key]
		kv.mu.Unlock()
		return
	}
}

// 和CLERK 通信的 RPC HANDLER
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	originOp := Op{args.Op, args.Key, args.Value, args.Cid, args.SeqNum}
	reply.WrongLeader = true
	// 向Raft发送消息
	// _,isLeader := kv.rf.GetState()的判断被包含在index,_,isLeader := kv.rf.Start(originOp)中
	index, _, isLeader := kv.rf.Start(originOp)
	if !isLeader {return}
	ch := kv.putIfAbsent(index)
	// 检测StartKVServer()中goroutine发来的chMap[idx]（index对应的Op数据）
	// 仅使用 op := <- ch 会阻塞，因此需要实现timeout逻辑
	op := beNotified(ch)
	// 如果两个Op相同，则回复领导人正确
	if equalOp(originOp,op) {
		reply.WrongLeader = false
	}
}

// 获取Op信道的消息，并返回一个Op值。如果超时，则返回空的Op，再进行equalOp时返回false，从而避免阻塞。
func beNotified(ch chan Op) Op{
	select {
	case op := <- ch :
		return op
	case <- time.After(time.Duration(600)*time.Millisecond):
		return Op{}
	}
}

// 如果命令所在的index没有对应的Op channel的话，创建它，返回chMap[idx]。(chMap形式为[<index1, chan Op>,<index2, chan Op>,...)]
func (kv *KVServer) putIfAbsent(idx int) chan Op{
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.chMap[idx]; !ok {
		kv.chMap[idx] = make(chan Op,1)  // 每个idx最多有1个msg，这里不想发生阻塞，所以设为1个缓冲区
	}
	return kv.chMap[idx]
}

// 判断两个Op是否相等
func equalOp(a Op, b Op) bool{
	return a.Key == b.Key && a.Value == b.Value && a.OpType == b.OpType
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
// KILL 掉一个KV SERVER
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	kv.killCh <- true
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer /*
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	// applyCh为raft回复消息到kvserve的channel,raft每应用一条命令，都会发送aplyMsg到applyCh channel
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)  // 启动一个raft节点
	kv.db = make(map[string]string)
	kv.chMap = make(map[int]chan Op)
	kv.cid2Seq = make(map[int64]int)  // 幂等
	kv.killCh = make(chan bool,1)
	// You may need initialization code here.
	// 启动一个GOROUTINE去监听applyCh, 每来一个命令都进行处理，找到applyMsg的CHANNEL，由Op NOTIFY
	go func() {
		for {
			select {
			// 实现kill
			case <-kv.killCh:
				return
			default:
			}
			applyMsg := <- kv.applyCh
			// 解析raft应用一条命令后发来的applyMsg，其中Op就是kvsever发给raft的命令
			op := applyMsg.Command.(Op)
			kv.mu.Lock()
			maxSeq, found := kv.cid2Seq[op.Cid]
			// 如果在服务器的记录中该客户端还没有请求，或者raft发回的applyMsg中的请求序号大于服务器记录的请求序号，则继续操作
			if !found || op.SeqNum > maxSeq {
				switch op.OpType {
				case "Put":
					kv.db[op.Key] = op.Value // 将op对应的键值put到kvDb
				case "Append":
					kv.db[op.Key] += op.Value // 将op对应的键值append到kvDb
					// "get"不会修改kvDb的键值
				}
				// 服务器更新客户端的序列号
				kv.cid2Seq[op.Cid] = op.SeqNum
			}
			kv.mu.Unlock()
			index := applyMsg.CommandIndex  // 该命令在log中的索引
			ch := kv.putIfAbsent(index)
			// 将从raft得到的command中的Op放入该index对应的chan Op中
			// 在sever的Get() handeler中, op := <- ch 这一行可以检测到chan Op中的信息
			ch <- op
		}
	}()

	return kv
}
