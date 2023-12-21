package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type ErrorCode int

const (
	kFileNotFound ErrorCode = iota + 1
	kWriteFailed
	kInvalidState
	kDeleteError
	kFailedRename
)

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

type CustomErrorCode struct {
	Code    ErrorCode
	Message string
}

type RegisterWorkerArgs struct {
	Addr string
}

type RegisterWorkerReply struct {
}

type MapTaskArgs struct {
	Shard   int64
	Fname   string
	NReduce int
}

type MapTaskReply struct {
	// Intermediate is only valid is Err is not set.
	Intermidate []string
	Err         CustomErrorCode
}

type DoneTaskArg struct {
}

type DoneTaskReply struct {
}

type ReduceTaskArgs struct {
	ID           int32
	Intermediate []string
}

type ReduceTaskReply struct {
	Result string
	Err    CustomErrorCode
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func workerSock() string {
	s := "/var/tmp/5840-mr-worker-"
	s += strconv.Itoa((os.Getpid()))
	return s
}
