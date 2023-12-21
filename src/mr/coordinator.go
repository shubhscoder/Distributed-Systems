package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	MAX_WORKERS = 50
	MAX_TASKS   = 100
	MAX_TIME    = 10
)

type Task struct {
	Shard  int64
	fname  string
	worker string
}

type ReduceTask struct {
	ID     int64
	worker string
}

type Coordinator struct {
	// Your definitions here.
	workers          chan string
	tasks            chan Task
	reduce_tasks     chan ReduceTask
	nmap             int
	nReduce          int
	mapcomplete      int32
	reducecomplete   int32
	intermediate     [][]string
	worker_alive     map[string]bool
	donechan         chan bool
	worker_alive_mut sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	c.worker_alive_mut.Lock()
	defer c.worker_alive_mut.Unlock()
	c.worker_alive[args.Addr] = true
	c.workers <- args.Addr
	return nil
}

func (c *Coordinator) InitalizeState() {
	c.workers = make(chan string, MAX_WORKERS)
	c.tasks = make(chan Task, MAX_TASKS)
	c.reduce_tasks = make(chan ReduceTask, MAX_TASKS)
	c.intermediate = make([][]string, c.nReduce)
	c.worker_alive = make(map[string]bool)
	c.donechan = make(chan bool)
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

func (c *Coordinator) SendReduceTask(worker string, red_task ReduceTask) {
	args := ReduceTaskArgs{}
	reply := ReduceTaskReply{}

	args.Intermediate = c.intermediate[red_task.ID]
	args.ID = int32(red_task.ID)
	ok := c.callWrapper("WorkerData.ReduceTask", worker, &args, &reply)
	if ok {
		// fmt.Printf("Reduce task %d completed\n", red_task.ID)
		c.workers <- worker
		atomic.AddInt32(&c.reducecomplete, 1)
		read_val := atomic.LoadInt32(&c.reducecomplete)

		if read_val == int32(c.nReduce) {
			close(c.reduce_tasks)
		}
	} else {
		c.worker_alive[worker] = false
		// fmt.Println("Task %s failed", red_task.ID)
		c.reduce_tasks <- red_task
	}
}

func (c *Coordinator) SendMapTask(worker string, task Task) {
	args := MapTaskArgs{}
	reply := MapTaskReply{}

	args.Fname = task.fname
	args.Shard = task.Shard
	args.NReduce = c.nReduce
	ok := c.callWrapper("WorkerData.MapTask", worker, &args, &reply)
	if ok {
		// fmt.Println("Task %s completed", task.fname)
		c.workers <- worker
		for i, fname := range reply.Intermidate {
			c.intermediate[i] = append(c.intermediate[i], fname)
		}

		atomic.AddInt32(&c.mapcomplete, 1)
		read_val := atomic.LoadInt32(&c.mapcomplete)

		// m map tasks
		// each mapper sends r files

		if read_val == int32(c.nmap) {
			close(c.tasks)
		}

	} else {
		c.worker_alive[worker] = false
		// fmt.Println("Task %s failed", task.fname)
		c.tasks <- task
	}
}

func rpccall(client *rpc.Client, method string, args interface{}, reply interface{}) <-chan error {
	errCh := make(chan error, 1)

	go func() {
		err := client.Call(method, args, reply)
		errCh <- err
	}()

	return errCh
}

func (c *Coordinator) callWrapper(rpcname string, worker string, args interface{}, reply interface{}) bool {
	client, err := rpc.DialHTTP("unix", worker)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer client.Close()

	timeout := MAX_TIME * time.Second
	timechan := time.After(timeout)

	select {
	case <-timechan:
		// fmt.Println("RPC call timed out")
		return false
	case err := <-rpccall(client, rpcname, args, reply):
		if err != nil {
			// fmt.Println("RPC failed with error :", err)
			return false
		}
		return true
	}
}

func (c *Coordinator) Scheduler() {
	// Send the map tasks.
	for task := range c.tasks {
		worker := <-c.workers
		c.SendMapTask(worker, task)
	}

	// fmt.Printf("MAP phase done.\n")

	// Map tasks are complete.
	// Populate the reduce tasks now
	for i := 0; i < c.nReduce; i++ {
		task := ReduceTask{}
		task.ID = int64(i)
		c.reduce_tasks <- task
	}

	for red_task := range c.reduce_tasks {
		worker := <-c.workers
		c.SendReduceTask(worker, red_task)
	}

	// fmt.Printf("REDUCE phase done.\n")

	for key, _ := range c.worker_alive {
		args := DoneTaskArg{}
		reply := DoneTaskReply{}

		// Ignore any errors in done call.
		c.callWrapper("WorkerData.DoneTask", key, &args, &reply)
	}

	// fmt.Print("DONE signaling done.\n")
	c.donechan <- true
	// Reduce tasks complete, now send done signal
}

func (c *Coordinator) SchedulerWrapper() {
	go c.Scheduler()
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {

	ret := <-c.donechan
	// fmt.Printf("Done : ret %v", ret)
	// Your code here.

	return ret
}

func (c *Coordinator) PopulateTasks(files []string) {
	c.nmap = len(files)
	for i := 0; i < c.nmap; i++ {
		cur_task := Task{}
		cur_task.fname = files[i]
		cur_task.Shard = int64(i)
		c.tasks <- cur_task
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nReduce = nReduce

	// fmt.Printf("Number of map tasks : %d\n", len(files))
	// fmt.Printf("Number of reduce tasks : %d\n", nReduce)

	c.InitalizeState()
	c.PopulateTasks(files)

	// Your code here.

	c.server()
	c.SchedulerWrapper()
	return &c
}
