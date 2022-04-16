package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strings"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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
	worker := worker{}
	worker.mapf = mapf
	worker.reducef = reducef

	// 向master注册worker，获取到分配的workerId序号
	worker.register()

	// 开启worker
	worker.run()

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

type worker struct {
	id int // worker序号Id
	mapf func(string, string) []KeyValue
	reducef func(string, []string) string
}

// 当前worker不断请求任务并执行
func (w *worker) run() {
	for {
		// 向Master请求任务
		task := w.askTask()

		if !task.Alive {
			DPrintf("worker ask task not alive, exit!")
			return
		}
		// 执行被分配的任务
		w.doTask(task)
	}
}

// 按类型（map or reduce）执行任务
func (w *worker) doTask(task Task){
	DPrintf("worker %d in doing task %d", w.id, task.Seq)

	switch task.Phase {
	case MapPhase:
		w.doMapTask(task)
	case ReducePhase:
		w.doReduceTask(task)
	default:
		panic(fmt.Sprintf("task phase err: %v", task.Phase))
	}
}

// 执行Map任务
func (w *worker) doMapTask(task Task){
	// 读取整个文件内容到内存中
	contents, err := ioutil.ReadFile(task.FileName)
	if err != nil {
		w.reportTask(task, false, err)
		return
	}

	// 调用自定义的map函数，生成一批键值对
	kvs := w.mapf(task.FileName, string(contents))

	// 键值对需要分为reducer数量的组，根据key的hash进行映射
	reduces := make([][]KeyValue, task.NReduce)
	for _, kv := range kvs {
		idx := ihash(kv.Key) % task.NReduce
		reduces[idx] = append(reduces[idx], kv)
	}

	// 构造出中间文件，每个mapper中都存有了nReduce数量的已分组好的中间文件
	for reduceIdx, kvs := range reduces {
		fileName := reduceName(task.Seq, reduceIdx)
		file, err := os.Create(fileName)
		if err != nil {
			w.reportTask(task, false, err)
		}
		enc := json.NewEncoder(file)
		// 向中间文件中填充键值对
		for _, kv := range kvs {
			if err := enc.Encode(&kv); err != nil {
				w.reportTask(task, false, err)
			}
		}
		if err := file.Close(); err != nil {
			w.reportTask(task, false, err)
		}
	}
	w.reportTask(task, true, nil)
}

// 执行Reduce任务
func (w *worker) doReduceTask(task Task){
	// 使用map存储每种key的所有键值对
	maps := make(map[string][]string)

	// 遍历属于该reducer的所有中间文件，聚合key
	for mapIdx := 0; mapIdx < task.NMaps; mapIdx++ {
		// 获取中间文件
		fileName := reduceName(mapIdx, task.Seq)
		file, err := os.Open(fileName)
		if err != nil {
			w.reportTask(task, false, err)
			return
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break				
			}
			if _, ok := maps[kv.Key]; !ok {
				maps[kv.Key] = make([]string, 0, 100)
			}
			// 聚合相同的key
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
	}

	// 使用自定义的reduce方法处理已聚合的键值对
	res := make([]string, 0, 100)
	for k, v := range maps {
		res = append(res, fmt.Sprintf("%v %v\n", k, w.reducef(k, v)))
	}

	// 将最后经过reduce方法处理后的结果写入到reducer的结果文件
	if err := ioutil.WriteFile(mergeName(task.Seq), []byte(strings.Join(res, "")), 0600); err != nil {
		w.reportTask(task, false, err)
	}

	w.reportTask(task, true, nil)
}


// 向master发起注册worker的rpc请求
func (w *worker) register() {
	args := &RegisterWorkerArgs{}
	reply := &RegisterWorkerReply{}

	if ok := call("Master.RegisterWorker", args, reply); !ok {
		log.Fatal("register worker fail")
	}

	w.id = reply.WorkerId
}

// 向master请求分配task
func (w *worker) askTask() Task {
	args := AskTaskArgs{}
	args.WorkerId = w.id

	reply := AskTaskReply{}

	if ok := call("Master.AskTask", &args, &reply); !ok {
		DPrintf("worker ask task fail, exit!")
		os.Exit(1)
	}

	DPrintf("worker %d ask task: %+v", args.WorkerId, reply.Task)

	return *reply.Task
}

// 向master报告任务执行情况
func (w *worker) reportTask(task Task, done bool, err error) {
	if err != nil {
		log.Printf("%v", err)
	}

	args := ReportTaskArgs{}
	args.Done = done
	args.Phase = task.Phase
	args.Seq = task.Seq
	args.WorkerId = w.id

	reply := ReportTaskReply{}

	if ok := call("Master.ReportTask", &args, &reply); !ok {
		DPrintf("report task fail: %+v", args)
	}
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
