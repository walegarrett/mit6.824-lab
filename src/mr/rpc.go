package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// 请求分配任务参数实体
type AskTaskArgs struct {
	WorkerId int
}

type AskTaskReply struct {
	Task *Task
}

// 请求响应任务执行结果实体
type ReportTaskArgs struct {
	Done bool
	Seq int // 任务的序号
	Phase TaskPhase
	WorkerId int // 执行该任务的worker
}

type ReportTaskReply struct {

}

// 向master注册worker的参数实体
type RegisterWorkerArgs struct {

}

type RegisterWorkerReply struct {
	WorkerId int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
