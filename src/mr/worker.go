package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
)

type WorkerData struct {
	// Your definitions here.
	addr     string
	mapper   func(string, string) []KeyValue
	reducer  func(string, []string) string
	donechan chan bool
}

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
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

func (w *WorkerData) serve() {
	rpc.Register(w)
	rpc.HandleHTTP()

	sockname := w.addr
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func getError(code ErrorCode, msg string) CustomErrorCode {
	return CustomErrorCode{
		Code:    code,
		Message: msg,
	}
}

func (w *WorkerData) signalDone() {
	w.donechan <- true
}

func (w *WorkerData) DoneTask(args *DoneTaskArg, reply *DoneTaskReply) error {
	defer w.signalDone()
	return nil
}

func (w *WorkerData) convertFile(file string, reply *ReduceTaskReply) ([]KeyValue, error) {
	file_hand, err := os.Open(file)
	if err != nil {
		reply.Err = getError(kFileNotFound, fmt.Sprintf("Unable to open file with error : %s", err))
		return nil, err
	}

	defer file_hand.Close()

	var key_val []KeyValue
	scanner := bufio.NewScanner(file_hand)

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) != 2 {
			err_msg := fmt.Sprintf("Invalid format of the intermediate file receieved after map phase")
			reply.Err = getError(kInvalidState, err_msg)
			return nil, fmt.Errorf(err_msg)
		}

		cur := KeyValue{
			Key:   parts[0],
			Value: parts[1],
		}

		key_val = append(key_val, cur)
	}

	if err := scanner.Err(); err != nil {
		err_msg := fmt.Sprintf("Scanner faced an error while reading the file : %s", err)
		reply.Err = getError(kInvalidState, err_msg)
		return nil, fmt.Errorf(err_msg)
	}

	return key_val, nil
}

func (w *WorkerData) ReduceTask(args *ReduceTaskArgs, reply *ReduceTaskReply) error {
	var intermediate []KeyValue

	for _, file := range args.Intermediate {
		key_val, err := w.convertFile(file, reply)
		if err != nil {
			return err
		}

		intermediate = append(intermediate, key_val...)
	}

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + strconv.Itoa(int(args.ID))
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := w.reducer(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()

	// fmt.Printf("Done create output file oname : %s\n", oname)

	return nil
}

func (w *WorkerData) MapTask(args *MapTaskArgs, reply *MapTaskReply) error {
	// fmt.Printf("MAP PID : %s\n", strconv.Itoa((os.Getpid())))
	files := make([]*os.File, args.NReduce)
	for i := 0; i < int(args.NReduce); i++ {
		// fname := "mr-" + strconv.Itoa(int(args.Shard)) + "-" + strconv.Itoa(int(i))
		fname := "mr-" + strconv.Itoa(os.Getpid()) + "-" + strconv.Itoa(int(args.Shard)) + "-" + strconv.Itoa(int(i))
		if _, err := os.Stat(fname); err == nil {
			// File exists, so delete it
			err := os.Remove(fname)
			if err != nil {
				fmt.Println("Error deleting file:", err)
				err_msg := fmt.Sprintf("Failed to delete file : %s with err %s", fname, err)
				reply.Err = getError(kDeleteError, err_msg)
				return fmt.Errorf(err_msg)
			}
		}

		reply.Intermidate = append(reply.Intermidate, fname)

		file, err := os.Create(fname)
		if err != nil {
			fmt.Println("Error creating file:", err)
			err_msg := fmt.Sprintf("Failed to create file : %s with err %s", fname, err)
			reply.Err = getError(kFileNotFound, err_msg)
			return fmt.Errorf(err_msg)
		}
		files[i] = file
	}

	// fmt.Println("Done creating intermediate files")

	// Open the file for reading.
	file, err := os.Open(args.Fname)
	if err != nil {
		log.Printf("Cannot open %v", args.Fname)
		err_msg := fmt.Sprintf("Failed to open file : %s with err %s", args.Fname, err)
		reply.Err = getError(kFileNotFound, err_msg)
		return fmt.Errorf(err_msg)
	}

	// Read its content
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", args.Fname)
	}
	file.Close()
	kva := w.mapper(args.Fname, string(content))

	for _, kv := range kva {
		key_val_str := fmt.Sprintf("%v %v\n", kv.Key, kv.Value)
		_, err = io.WriteString(files[ihash(kv.Key)%int(args.NReduce)], key_val_str)
		if err != nil {
			err_msg := fmt.Sprintf("Failed to write to file key value pair to file with err : %s", err)
			reply.Err = getError(kWriteFailed, err_msg)
			return fmt.Errorf(err_msg)
		}
	}

	for i := 0; i < args.NReduce; i++ {
		files[i].Close()
	}

	// rename all files now
	for i := 0; i < args.NReduce; i++ {
		old_name := "mr-" + strconv.Itoa(os.Getpid()) + "-" + strconv.Itoa(int(args.Shard)) + "-" + strconv.Itoa(int(i))
		new_name := "mr-" + strconv.Itoa(int(args.Shard)) + "-" + strconv.Itoa(int(i))
		err := os.Rename(old_name, new_name)
		if err != nil {
			err_msg := fmt.Sprintf("Failed to rename file %s to %s", old_name, new_name)
			reply.Err = getError(kFailedRename, err_msg)
			return fmt.Errorf(err_msg)
		}
	}
	return nil
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	worker_obj := WorkerData{}
	worker_obj.addr = workerSock()
	worker_obj.mapper = mapf
	worker_obj.reducer = reducef
	worker_obj.donechan = make(chan bool)

	worker_obj.serve()
	// register worker with coordinator.
	register_args := RegisterWorkerArgs{}
	register_reply := RegisterWorkerReply{}
	register_args.Addr = worker_obj.addr
	ok := call("Coordinator.RegisterWorker", &register_args, &register_reply)
	if ok {
		// reply.Y should be 100.
		// fmt.Printf("Done registration\n")
	} else {
		fmt.Printf("call failed!\n")
	}

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	<-worker_obj.donechan
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
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
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
