package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	files   []string
	nMap    int
	nReduce int

	nMapDone    int
	nReduceDone int

	phase            string  // Mapping | Reducing | Done.  When all files are mapped, go to reducing phase
	mapStatus        []int64 // record a map task is assigned to a request。 0 means not assigned, 1 means done
	reduceStatus     []int64 // record a reduce task is assigned to a request
	mapUnAssigned    []int
	reduceUnAssigned []int

	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AskForTask(request *Request, reply *Reply) error {
	reply.ReplyId = request.RequestId
	c.mu.Lock()
	if c.phase == "Mapping" {
		if len(c.mapUnAssigned) == 0 {
			reply.ReplyType = "Wait"
			c.mu.Unlock()
		} else {
			mapIdToAssign := c.mapUnAssigned[0]
			c.mapUnAssigned = c.mapUnAssigned[1:]
			c.mapStatus[mapIdToAssign] = reply.ReplyId

			reply.ReplyType = "Map"
			reply.MapFile = c.files[mapIdToAssign]
			reply.MapId = mapIdToAssign
			reply.NReduce = c.nReduce
			//fmt.Printf("reply.nReduce: %v, c.nReduce: %v\n", reply.NReduce, c.nReduce)
			c.mu.Unlock()

			go tenSecResponseCheckt(c, mapIdToAssign, "Map")
		}
	} else if c.phase == "Reducing" {
		if len(c.reduceUnAssigned) == 0 {
			reply.ReplyType = "Wait"
			c.mu.Unlock()
		} else {
			reduceIdToAssign := c.reduceUnAssigned[0]
			c.reduceUnAssigned = c.reduceUnAssigned[1:]
			c.reduceStatus[reduceIdToAssign] = reply.ReplyId

			reply.ReplyType = "Reduce"
			reply.ReduceId = reduceIdToAssign
			reply.NMap = c.nMap
			c.mu.Unlock()

			go tenSecResponseCheckt(c, reduceIdToAssign, "Reduce")
		}
	} else {
		reply.ReplyType = "Done"
		c.mu.Unlock()
	}

	return nil
}

func tenSecResponseCheckt(c *Coordinator, taskId int, taskType string) {
	time.Sleep(time.Second * 10)
	c.mu.Lock()
	defer c.mu.Unlock()
	if taskType == "Map" && c.mapStatus[taskId] != 1 {
		c.mapStatus[taskId] = 0
		c.mapUnAssigned = append(c.mapUnAssigned, taskId)
	} else if taskType == "Reduce" && c.reduceStatus[taskId] != 1 {
		c.reduceStatus[taskId] = 0
		c.reduceUnAssigned = append(c.reduceUnAssigned, taskId)
	}
}

func (c *Coordinator) TellDone(request *Request, reply *Reply) error {
	reply.ReplyId = request.RequestId
	c.mu.Lock()
	defer c.mu.Unlock()
	// fmt.Println("=======TellDone======\n===>Request:\n" + request.String())
	// fmt.Println(c.mapStatus)
	if c.phase == "Mapping" && request.TaskType == "Map" {
		if c.mapStatus[request.TaskId] == request.RequestId {
			c.nMapDone++
			c.mapStatus[request.TaskId] = 1
			if c.nMapDone == c.nMap {
				c.phase = "Reducing"
			}
		}
	} else if c.phase == "Reducing" && request.TaskType == "Reduce" {
		if c.reduceStatus[request.TaskId] == request.RequestId {
			c.nReduceDone++
			c.reduceStatus[request.TaskId] = 1
			if c.nReduceDone == c.nReduce {
				c.phase = "Done"
			}
		}
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	ret = c.phase == "Done"
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files
	c.nMap = len(files)
	c.nReduce = nReduce
	c.nMapDone = 0
	c.nReduceDone = 0
	c.phase = "Mapping"
	c.mapStatus = make([]int64, c.nMap)
	c.reduceStatus = make([]int64, c.nReduce)
	c.mapUnAssigned = make([]int, 0)
	c.reduceUnAssigned = make([]int, 0)
	for i := 0; i < c.nMap; i++ {
		c.mapUnAssigned = append(c.mapUnAssigned, i)
	}
	for i := 0; i < c.nReduce; i++ {
		c.reduceUnAssigned = append(c.reduceUnAssigned, i)
	}

	//fmt.Printf("Making coordinator, reduce number : %v, \nmapUnAssigned: %v, reduceUnAssigned: %v\n", c.nReduce, c.mapUnAssigned, c.reduceUnAssigned)

	c.server()
	return &c
}
