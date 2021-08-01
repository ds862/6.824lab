package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"

// 定义Clerk的属性
type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader  int  // 上一次请求的leader
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// 新建一个CLERK，同时会告知一组SERVER
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.lastLeader = 0
	return ck
}

/*
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
获取键的当前值。如果键不存在，返回""。即使发生错误，也要保持尝试。
你可以这样发送RPC: ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
args和reply的类型(包括它们是否是指针)必须匹配RPC处理函数参数声明的类型。并且reply必须作为一个指针传递。
 */
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	index := ck.lastLeader
	// 使用for循环，直到找到正确的Leader
	for {
		args := GetArgs{key}
		reply := GetReply{}  // reply要放在循环中，因为每台服务器都会reply，下一次循环时需要重置
		ok := ck.servers[index].Call("KVServer.Get", &args, &reply)
		if ok && !reply.WrongLeader {
			ck.lastLeader = index
			return reply.Value
		}
		// 使用取余的方式增加服务器id，转向下一个服务器
		index = (index + 1) % len(ck.servers)
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
// 发送PUT 或 APPEND请求, 要求同Get()
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	index := ck.lastLeader
	// 使用for循环，直到找到正确的Leader
	for {
		args := PutAppendArgs{
			key,
			value,
			op,
		}
		reply := PutAppendReply{}
		ok := ck.servers[index].Call("KVServer.PutAppend", &args, &reply)
		if ok && !reply.WrongLeader {
			ck.lastLeader = index
			return
		}
		index = (index + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
