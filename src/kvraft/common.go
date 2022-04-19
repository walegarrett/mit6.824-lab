package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut = "ErrTimeOut"
)

type Err string

type msgId int64

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	// Clerk通过（ClientId，MsgId）唯一标识了一个请求

	MsgId msgId
	ClientId int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.

	// Clerk通过（ClientId，MsgId）唯一标识了一个请求

	MsgId msgId
	ClientId int64
}

type GetReply struct {
	Err   Err
	Value string
}
