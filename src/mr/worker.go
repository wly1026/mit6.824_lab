package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// continously ask for assignment until all assignments are finished
	for {
		reply := AskForTask()
		switch reply.ReplyType {
		case "Map":
			DoMapTask(reply, mapf)
		case "Reduce":
			DoReduceTask(reply, reducef)
		case "Wait":
			Wait()
		case "Done":
			return
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

// return a fliename to do map task
func AskForTask() *Reply {
	args := Request{RequestId: time.Now().Unix()}
	reply := Reply{}
	ok := call("Coordinator.AskForTask", &args, &reply)
	if !ok {
		log.Fatal("ask for task call failed!\n")
	}
	//fmt.Println("=======AskForTask======\n===>Request:\n" + args.String() + "\n===>Reply:\n" + reply.String())
	return &reply
}

// tell the coordinator a work has been finished
func TellDone(requestId int64, taskId int, taskType string) {
	args := Request{requestId, taskId, taskType}
	reply := Reply{}
	ok := call("Coordinator.TellDone", &args, &reply)
	if !ok {
		log.Fatal("tell done call failed!")
	}

	//fmt.Println("=======TellDone======\n===>Request:\n" + args.String() + "\n===>Reply:\n" + reply.String())
}

// do the map task. read the file, store it to the intermediate file
func DoMapTask(reply *Reply, mapf func(string, string) []KeyValue) {
	// read file, genrate keyValue slice
	file, err := os.Open(reply.MapFile)
	defer file.Close()

	if err != nil {
		log.Fatalf("cannot open %v", reply.MapFile)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.MapFile)
	}
	kvs := mapf(reply.MapFile, string(content))

	// create n files. n is the number reduce tasks
	files := make([]*os.File, reply.NReduce)
	for i, _ := range files {
		fname := "mr-" + strconv.Itoa(reply.MapId) + "-" + strconv.Itoa(i)
		files[i], err = os.Create(fname)
		defer files[i].Close()
	}

	// write all keyvalue to these files according to hash
	for _, kv := range kvs {
		currReduce := ihash(kv.Key) % reply.NReduce
		enc := json.NewEncoder(files[currReduce])
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatalf("cannot write keyValue from %vth map file to %vth reduce", reply.MapId, currReduce)
		}
	}

	// notice the coorinator a work is done
	TellDone(reply.ReplyId, reply.MapId, "Map")
}

// Do the id y-th reduce task, generate file mr-out-y.
// All files mr-x-y, x is [0, nMap) are decoded back to keyValue pair, and then sort.
// call reducef to get the cnt of a key, write to the mr-out-y
func DoReduceTask(reply *Reply, reducef func(string, []string) string) {
	// get all keyValue
	kvs := []KeyValue{}
	for i := 0; i < reply.NMap; i++ {
		intermediateFileName := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.ReduceId)
		file, err := os.Open(intermediateFileName)
		defer file.Close()
		if err != nil {
			log.Fatalf("cannot open intermediateFile %v", intermediateFileName)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
	}

	// sort all keyValues
	sort.Sort(ByKey(kvs))

	// write to the output file
	oname := "mr-out-" + strconv.Itoa(reply.ReduceId)
	ofile, _ := os.Create(oname)
	defer ofile.Close()

	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		output := reducef(kvs[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kvs[i].Key, output)

		i = j
	}

	// notice the coorinator a work is done
	TellDone(reply.ReplyId, reply.ReduceId, "Reduce")
}

func Wait() {
	time.Sleep(time.Second)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234"
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
