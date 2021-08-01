package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Cid    int64  // 每个客户端的唯一标识符
	SeqNum int    // 用单调递增的序列号标记每个请求。
}

type PutAppendReply struct {
	Err Err
	WrongLeader bool  // 3A
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
	WrongLeader bool  // 3A
}
