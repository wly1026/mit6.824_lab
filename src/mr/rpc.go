package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"strconv"
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

// Add your RPC definitions here.

type Request struct {
	RequestId int64 // time stamp to mark the unique id for each request
	// for tell done request
	TaskId   int    // mapId or reduce Id
	TaskType string // Map | Reduce
}

type Reply struct {
	ReplyId   int64  // pass the request id
	ReplyType string // Map | Reduce | Done | Wait
	// for map task
	MapFile string // the name of file
	MapId   int    // the idx of map task
	NReduce int    // the number of reduce task
	// for reduce task
	ReduceId int // the id of reduce work
	NMap     int // the number map
}

func (r Reply) String() string {
	return fmt.Sprintf("ReplyId: %v, ReplyType: %v\nMapFile: %v, MapId: %v, nReduce: %v\nReduceId: %v, nMap: %v",
		r.ReplyId, r.ReplyType, r.MapFile, r.MapId, r.NReduce, r.ReduceId, r.NMap)
}

func (r Request) String() string {
	return fmt.Sprintf("RequestId: %v, TaskId: %v, TaskType: %v", r.RequestId, r.TaskId, r.TaskType)
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
