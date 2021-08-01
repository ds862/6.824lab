package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "sync/atomic"
import "../labrpc"
import "sort"
import "math/rand"
import "time"
// import "bytes"
// import "../labgob"



/*
// as each Raft peer becomes aware that successive log entries are
//// committed, the peer should send an ApplyMsg to the service (or
//// tester) on the same server, via the applyCh passed to Make(). set
//// CommandValid to true to indicate that the ApplyMsg contains a newly
//// committed log entry.
////
//// in Lab 3 you'll want to send other kinds of messages (e.g.,
//// snapshots) on the applyCh; at that point you can add fields to
//// ApplyMsg, but set CommandValid to false for these other uses.

当每个Raft节点意识到连续的日志条目被提交时，
该节点应该通过传递给Make()的applyCh向同一服务器上的服务(或测试人员)发送一个ApplyMsg。
设置CommandValid为true表示ApplyMsg包含一个新提交的日志条目。

在实验3中，你会想在applyCh上发送其他类型的消息(例如，快照);
此时，您可以向ApplyMsg添加字段，但将CommandValid设置为false用于其他用途。
*/
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// 使用const＋iota实现枚举，从而表明一台RAFT SERVER的状态
type State int
const (
	Follower State = iota // value --> 0
	Candidate             // value --> 1
	Leader                // value --> 2
)

const NULL int = -1

type Log struct {
	Term    int         "term when entry was received by leader"
	Command interface{} "command for state machine,"
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       State         // 节点状态：Follower、Candidate、Leader，所有服务器
	// 所有服务器上的持久性状态
	currentTerm int           // 2A，服务器已知的最新term(从0开始递增)
	votedFor    int           // 2A，当前term内收到选票的候选者id(给哪个候选者投的票)，没有投票则为空
	log         []Log         // 2A, 日志条目（第一个索引为1）
	// 所有服务器上的易失性状态
	commitIndex int           // 2B, 已知已提交的最高的log的索引，初始值为0
	lastApplied int           // 2B, 已经被应用到状态机的最高的log的索引，初始值为0
	// Leader上的易失性状态
	nextIndex   []int         // 2B, 对于其他每一台服务器，发送给他们的下一条log的索引（初始值为Leader最后日志条目的索引+1）
	matchIndex  []int         // 2B, 对于其他每一台服务器，已知已经匹配的最高log索引（从0开始递增）
    // channel
	applyCh     chan ApplyMsg // 2B,
	// handler rpc
    voteCh      chan bool
	appendLogCh chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	// 注意访问节点状态时需要加锁，可以使用defer语句将解锁推迟到函数返回前执行
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// 将Raft的持久状态保存到稳定存储中，在崩溃和重新启动后可以在那里检索它。关于什么应该被持久化，请参见论文的图2。
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
// 恢复之前保存的状态。
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
    Term         int    // 2A, 候选人的任期号
	CandidateId  int    // 2A, 请求选票的候选人id
	LastLogIndex int    // 2B, 候选人最新日志条目的索引值
	LastLogTerm  int    // 2B, 候选人最新日志条目对应的任期号
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int      // 2A, 节点的当前任期号，以便更新落后候选人的任期号
	VoteGranted bool     // 2A, 候选人赢得此张选票时为真
}


// example RequestVote RPC handler.(在节点中执行)
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// all server rule 1 If RPC request or response contains term T > currentTerm:
	if (args.Term > rf.currentTerm) {
		rf.beFollower(args.Term)  // set currentTerm = T, convert to follower (§5.1)
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	// 不投票: 1.Candidate的term小于节点的currentTrem 2.节点已经投过票，并且已经投票的对象不是发起该次请求的候选人
	if (args.Term < rf.currentTerm) || (rf.votedFor != NULL && rf.votedFor != args.CandidateId) {
		return
	// 不投票: 候选人的日志没有节点新
	// 如果两份日志中最后一个条目的任期号不同，那么任期号大的日志更加新。
	// 如果两份日志中最后一个条目的任期号相同，则日志越长越新。
	} else if args.LastLogTerm < rf.getLastLogTerm() ||
		(args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex < rf.getLastLogIdx()) {
		return
    // 投票
	} else {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.state = Follower
		// 投票后要重置选举定时器
		send(rf.voteCh)
	}
}


// 追加日志rpc，被Leader调用
type AppendEntriesArgs struct {
	Term         int        // 2A, Leader的任期号
	LeaderId     int        // 2A, Leader的id，因此Follower可以对客户端重定向
	PrevLogIndex int        // 2B, 紧邻新log之前的那个日志条目的索引
	PrevLogTerm  int        // 2B, 紧邻新log之前的那个日志条目的term
	Entries      []Log      // 2B, 需要被保存的日志条目(可以发送多个)
	LeaderCommit int        // 2B, Leader已提交的最高日志条目的索引
}

type AppendEntriesReply struct {
	Term         int        // 2A，节点的当前任期号，以便更新落后候选人的任期号
	Success      bool       // 2A，如果Follower所含有的日志条目和PrevLogIndex以及PrevLogTerm匹配上了，则为真
}

// AppendEntries RPC handler.
// now only for heartbeat
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 如果选举超时后没有从当前leader接收到AppendEntries RPC
	defer send(rf.appendLogCh)
	// all server rule 1 If RPC request or response contains term T > currentTerm:
	if args.Term > rf.currentTerm {
		rf.beFollower(args.Term) // set currentTerm = T, convert to follower (§5.1)
	}
	reply.Term = rf.currentTerm
	reply.Success = false
	// 1. 返回假，如果领导者的任期 小于 接收者的当前任期（5.1 节）
	if args.Term < rf.currentTerm {return}
	//2. 返回假，如果接收者日志中没有包含这样一个条目:该条目在prevLogIndex上的Term和prevLogTerm匹配 (§5.3)
	prevLogIndexTerm := -1
	if args.PrevLogIndex >= 0 && args.PrevLogIndex < len(rf.log) {
		prevLogIndexTerm = rf.log[args.PrevLogIndex].Term
	}
	if prevLogIndexTerm != args.PrevLogTerm {
		return
	}
    // 3.如果一个已经存在的条目和新条目发生了冲突(因为索引相同,任期不同),那么就删除这个存在的条目以及它之后的所有条目(5.3节)
	index := args.PrevLogIndex
	for i := 0; i < len(args.Entries); i++ {
		index++
		if index >= len(rf.log) {
			//4. 追加日志中尚未存在的任何新条目
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
		if rf.log[index].Term != args.Entries[i].Term {
			rf.log = rf.log[:index]
			//4. 追加日志中尚未存在的任何新条目
			rf.log = append(rf.log,args.Entries[i:]...)
			break
		}
	}
	// 5.如果领导者的已知已经提交的最高的日志条目的索引leaderCommit 大于 接收者的已知提交的最高日志条目的索引commitIndex,
	// 则把接收者的commitIndex 重置为 min(leaderCommit, 上一个新条目的索引)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit ,rf.getLastLogIdx())
		rf.updateLastApplied()
	}
	reply.Success = true
}

func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

/*
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
发送RequestVote RPC到服务器的示例代码。Server是目标服务器在rf.peers[]中的索引。
args为RPC的参数。用RPC reply填充*reply，所以调用方应该传递&reply。
传递给Call()的参数和应答的类型必须与处理函数中声明的参数的类型相同(包括它们是否为指针)。
labrpc包模拟了一个有损网络，其中服务器可能不可达，请求和响应可能丢失。
Call()发送一个请求并等待应答。
如果应答在超时时间内到达，Call()返回true;否则Call()返回false。因此Call()可能在一段时间内不会返回。
错误返回可能是由服务器失效、无法访问的活动服务器、丢失的请求或丢失的应答引起的。
Call()保证会返回(可能在延迟之后)，除非服务器端的handler函数没有返回。因此，不需要在Call()周围实现自己的超时。
看看../labrpc/labrpc中的注释。了解更多细节。
如果RPC不能工作，在传递RPC的structs中检查已经大写了的所有字段名，并且调用者传递回复结构的地址(使用&)，而不是结构本身。
*/
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func send(ch chan bool) {
	select {
	case <-ch: //if already set, consume it then resent to avoid block
	default:
	}
	ch <- true
}

// Follower Section:
func (rf *Raft) beFollower(term int) {
	rf.state = Follower
	rf.votedFor = NULL
	rf.currentTerm = term
}  // end Follower section

//Candidate Section:
// 如果AppendEntries RPC接收到新的Leader:转换为Follower（在AppendEntries RPC Handler中实现）
// 在调用者中重置选举定时器
func (rf *Raft) beCandidate() {
	rf.state = Candidate
	rf.currentTerm++   // 增加currentTerm
	rf.votedFor = rf.me  // 先给自己投票
	// 向其他节点发送RequestVote rpc，征求选票
	go rf.startElection()
}


func (rf *Raft) beLeader() {
	if rf.state != Candidate {
		return
	}
	rf.state = Leader
	// 初始化Leader的易失性状态
	rf.nextIndex = make([]int,len(rf.peers))
	rf.matchIndex = make([]int,len(rf.peers))  // initialized to 0
	for i := 0; i < len(rf.nextIndex); i++ {   // nextIndex[]初始化为leader的上一条日志索引 + 1
		rf.nextIndex[i] = rf.getLastLogIdx() + 1
	}
}  //end Leader section


func (rf *Raft) getLastLogIdx() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLastLogTerm() int {
	idx := rf.getLastLogIdx()
	if idx < 0 {
		return -1
	}
	return rf.log[idx].Term
}

func (rf *Raft) getPrevLogIdx(i int) int {
	return rf.nextIndex[i] - 1
}

func (rf *Raft) getPrevLogTerm(i int) int {
	prevLogIdx := rf.getPrevLogIdx(i)
	if prevLogIdx < 0 {
		return -1
	}
	return rf.log[prevLogIdx].Term
}

// If election timeout elapses: start new election handled in caller
func (rf *Raft) startElection() {
	rf.mu.Lock()
	args := RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		rf.getLastLogIdx(),
		rf.getLastLogTerm(),
	}
	rf.mu.Unlock()
	var votes int32 = 1;
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(idx int) {
			reply := &RequestVoteReply{}
			// 发送rpc
			ret := rf.sendRequestVote(idx,&args,reply)
			if ret {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// 收到的选票结果中Term大于节点自身的Term,说明该节点信息已过期，
				// 将currentTerm置为选票结果中的Term，并且节点转为Follower
				if reply.Term > rf.currentTerm {
					rf.beFollower(reply.Term)
					// send(rf.voteCh)  // 此时不发送rpc的原因是，return后会立即开始下一轮for循环
					return
				}
				if rf.state != Candidate || rf.currentTerm != args.Term{
					return
				}
				if reply.VoteGranted {
					atomic.AddInt32(&votes,1)
				}  // 得到半数以上的选票，节点就转为Leader
				if atomic.LoadInt32(&votes) > int32(len(rf.peers) / 2) {
					rf.beLeader()
					//在成为leader之后，通知'select' goroutine立即发送心跳
					send(rf.voteCh)
				}
			}
		}(i)
	}
}  // end Candidate section

// Leader Section:
func (rf *Raft) startAppendLog() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(idx int) {
			for {
				rf.mu.Lock()
				if rf.state != Leader {
					rf.mu.Unlock()
					return
				}
				// send initial empty AppendEntries RPCs (heartbeat) to each server
				args := AppendEntriesArgs{
					rf.currentTerm,
					rf.me,
					rf.getPrevLogIdx(idx),
					rf.getPrevLogTerm(idx),
					// 对于一个跟随者idx，Leader要发送给他的日志条目为：从nextIndex[idx]到日志结尾
					append(make([]Log,0),rf.log[rf.nextIndex[idx]:]...),
					rf.commitIndex,
				}
				rf.mu.Unlock()
				reply := &AppendEntriesReply{}
				ret := rf.sendAppendEntries(idx, &args, reply)
				rf.mu.Lock()
				if !ret || rf.state != Leader || rf.currentTerm != args.Term {
					rf.mu.Unlock()
					return
				}
				// all server rule 1 If RPC response contains term T > currentTerm:
				if reply.Term > rf.currentTerm {
					rf.beFollower(reply.Term)
					rf.mu.Unlock()
					return
				}
				// 如果节点回复成功：更新对该节点的 nextIndex 和 matchIndex, 提交该索引的日志
				if reply.Success {
					rf.matchIndex[idx] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[idx] = rf.matchIndex[idx] + 1
					rf.updateCommitIndex()
					rf.mu.Unlock()
					return
				} else {  // 如果由于日志不一致而失败:递减nextIndex，然后重试
					rf.nextIndex[idx]--
					rf.mu.Unlock()
				}
			}
		}(i)
	}
}

// 提交当前Term的日志
func (rf *Raft) updateCommitIndex() {
	rf.matchIndex[rf.me] = len(rf.log) - 1
	copyMatchIndex := make([]int,len(rf.matchIndex))
	copy(copyMatchIndex,rf.matchIndex)
	// 需要反向排序，也就是大的在前面
	sort.Sort(sort.Reverse(sort.IntSlice(copyMatchIndex)))
	// 领导人遵守的规则4
	// len(copyMatchIndex)/2 会向下取整(两个int相除)，如长度为5的切片 ，结果为2，但切片索引从0开始，因此N为最中间的索引值
	N := copyMatchIndex[len(copyMatchIndex)/2]
	// 注意，需要检查N处log的Term是否等于当前任期Term，因为Leader不能提交之前Term的日志
	if N > rf.commitIndex && rf.log[N].Term == rf.currentTerm {
		rf.commitIndex = N
		rf.updateLastApplied()
	}
}

// 所有服务器规则1：如果 commitIndex > lastApplied，lastApplied + 1, 并应用 log[lastApplied] 到状态机
func (rf *Raft) updateLastApplied() {
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		curLog := rf.log[rf.lastApplied]
		applyMsg := ApplyMsg{
			true,
			curLog.Command,
			rf.lastApplied,
		}
		rf.applyCh <- applyMsg
	}
}

/*
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
总结：Start()的功能是将接收到的客户端命令追加到自己的本地log，
然后给其他所有peers并行发送AppendEntries RPC来迫使其他peer也同意领导者日志的内容，
在收到大多数peers的已追加该命令到log的肯定回复后，
若该entry的任期等于leader的当前任期，则leader将该entry标记为已提交的(committed)，
提升(adavance)`commitIndex`到该entry所在的`index`，并发送`ApplyMsg`消息到`ApplyCh`，
相当于应用该entry的命令到状态机。
*/
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := (rf.state == Leader)
	// 如果从客户端接收到命令:将日志条目附加到本地日志，在条目应用到状态机后响应(§5.3)
	if isLeader {
		index = rf.getLastLogIdx() + 1  // 初始从 1 开始;log的初始长度为1，getLastLogIdx()为0, 加1后index为1
		newLog := Log{
			rf.currentTerm,
			command,
		}
		rf.log = append(rf.log,newLog)
	}
	return index, term, isLeader
}


//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

/*
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.

service 或 tester想要创建一个Raft服务器。所有Raft服务器(包括此服务器)的端口都在peers[]中。
这个服务器自己的端口是peers[me]。所有服务器的peers[]数组具有相同的顺序。
Persister是这个服务器保存其持久状态的地方，并且最初也保存最近保存的状态(如果有的话)。
applyCh是一个通道，测试人员或服务希望Raft在该通道上发送ApplyMsg消息。
Make()必须快速返回，因此它应该为任何长时间运行的工作启动goroutine。
*/
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	// 持久性状态
	rf.currentTerm = 0
	rf.votedFor = NULL
	rf.log = make([]Log,1)  // 注意，log的第一个索引为 1。因此这里长度设为 1，调用start()后index的初始值便为1
	// 易失性状态
	rf.commitIndex = 0
	rf.lastApplied = 0
	// Leader上的易失性
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
    // channel
    rf.applyCh = applyCh
    // 因为goroutine只发送chan到下面的goroutine，为了避免阻塞，需要1个缓冲区
    rf.voteCh = make(chan bool, 1)
    rf.appendLogCh = make(chan bool, 1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

    // 心跳超时时间，tester要求leader每秒发送心跳rpc不超过10次。
	heartbeatTime := time.Duration(100) * time.Millisecond
	// from hint : 您需要编写定期或延迟后执行操作的代码。
	// 最简单的方法是创建一个带有调用time.Sleep()循环的goroutine。
	go func() {
		for {
			// 选举超时时间，因为是随机选取，所以要在协程中定义
			electionTime := time.Duration(rand.Intn(200) + 300) * time.Millisecond
			// 读取状态要加锁
			rf.mu.Lock()
			state := rf.state
			rf.mu.Unlock()
			switch state {
			case Follower, Candidate:
				select {
				case <-rf.voteCh:
				case <-rf.appendLogCh:
				case <-time.After(electionTime):
					rf.mu.Lock()
					rf.beCandidate()  // 转换为Candidate，然后开始选举
					rf.mu.Unlock()
				}
			case Leader:
				rf.startAppendLog()
				time.Sleep(heartbeatTime)
			}
		}
	}()
	return rf
}
