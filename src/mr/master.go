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

const (
	TaskStatusReady = 0 // 默认的状态，准备状态
	TaskStatusQueue = 1 // 待分配worker的状态
	TaskStatusRunning = 2
	TaskStatusFinish = 3
	TaskStatusErr = 4
)

const (
	// 调度间隔时间
	ScheduleInteereval = time.Millisecond * 500
	// 任务的最大运行时间
	MaxTaskRunTime = time.Second * 5
)

// 已分配worker的task的状态信息
type TaskStatInfo struct{
	Status int // 当前任务的状态
	WorkerId int // 运行当前任务的worker
	StartTime time.Time // 任务的开始时间
}

type Master struct {
	// Your definitions here.
	files []string // 当前job需要处理的所有输出文件
	nReduce int // reducer数量，也就是输出文件的数量
	taskPhase TaskPhase // 当前job在当前时刻的整体任务类型，map或者reduce
	taskStatInfo []TaskStatInfo // 当前job在当前阶段的所有任务的状态信息
	workerSeq int // 所有已注册的worker的数量
	taskQueued chan Task // 存放待分配worker的task，是一个任务队列
	done bool // 当前job的当前阶段是否完成，分为map和reduce
	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// 注册worker
func (m *Master) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// master中维护所有的worker数量
	m.workerSeq += 1
	reply.WorkerId = m.workerSeq
	return nil
}

// worker请求任务，master需要分配task给worker
func (m *Master) AskTask(args *AskTaskArgs, reply *AskTaskReply) error {
	// master从任务队列中获取一个待分配的任务
	task := <-m.taskQueued
	reply.Task = &task

	// 填充worker相关的信息以及修改任务的状态和设置开始时间
	if task.Alive {
		m.registerTask(args, &task)
	}

	DPrintf("in ask task, args:%+v, reply:%+v", args, reply)

	return nil
}

// worker报告任务的完成情况
func (m *Master) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	DPrintf("report task: %+v, taskPhase: %+v", args, m.taskPhase)

	if m.taskPhase != args.Phase || args.WorkerId != m.taskStatInfo[args.Seq].WorkerId {
		return nil
	}

	if args.Done {
		m.taskStatInfo[args.Seq].Status = TaskStatusFinish
	} else {
		m.taskStatInfo[args.Seq].Status = TaskStatusErr
	}

	// 因为修改了任务信息，重新开启调度，轮询当前job的当前阶段的情况
	go m.schedule()

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.done
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.mu = sync.Mutex{}
	m.nReduce = nReduce
	m.files = files

	// 任务队列的创建，取mapper和reducer的最大值
	if nReduce > len(files) {
		m.taskQueued = make(chan Task, nReduce)
	} else {
		m.taskQueued = make(chan Task, len(m.files))
	}

	// 首先初始化map任务
	m.initMapTask()

	// 开启定时任务调度
	go m.tickSchedule()

	// 开启rpc服务，启动监听worker的相关请求
	m.server()

	DPrintf("master init")
	
	return &m
}

func (m *Master) initMapTask() {
	m.taskPhase = MapPhase
	// msp task数组的创建
	m.taskStatInfo = make([]TaskStatInfo, len(m.files))
}

func (m *Master) initReduceTask() {
	m.taskPhase = ReducePhase
	// reduce task数组的创建
	m.taskStatInfo = make([]TaskStatInfo, m.nReduce)
}

// 开启定时调度
func (m *Master) tickSchedule() {
	for !m.Done() {
		go m.schedule()
		time.Sleep(ScheduleInteereval)
	}
}

func (m *Master) schedule() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.done {
		return
	}

	// 当前阶段（map or reduce）的任务是否都已经完成了
	allFinish := true

	for index, task := range m.taskStatInfo {
		switch task.Status {
		case TaskStatusReady:
			allFinish = false
			m.taskQueued <- m.createTask(index)
			m.taskStatInfo[index].Status = TaskStatusQueue
		case TaskStatusQueue:
			allFinish = false
		case TaskStatusRunning:
			allFinish = false
			// 检查任务已经运行时间是否超出设置的最大时间阈值
			if time.Now().Sub(task.StartTime) > MaxTaskRunTime {
				m.taskStatInfo[index].Status = TaskStatusQueue
				m.taskQueued <- m.createTask(index)
			}
		case TaskStatusFinish:
		case TaskStatusErr:
			allFinish = false
			m.taskStatInfo[index].Status = TaskStatusQueue
			m.taskQueued <- m.createTask(index)
		default:
			panic("task.Status err")
		}
	}

	if allFinish {
		// map阶段结束后开启reduce阶段
		if m.taskPhase == MapPhase {
			m.initReduceTask()
		} else {
			// 只有reduce阶段也完成了才表示当前job已经完成
			m.done = true
		}
	}
}

// 创建一个新的任务实体，任务的序号为taskStatInfo的对应索引
func (m *Master) createTask(taskSeq int) Task {
	task := Task {
		FileName: "",
		NReduce: m.nReduce,
		NMaps: len(m.files),
		Seq: taskSeq,
		Phase: m.taskPhase,
		Alive: true,
	}

	DPrintf("master:%+v, taskSeq:%d, lenFiles:%d, lenTaskStatInfo:%d", 
					m, taskSeq, len(m.files), len(m.taskStatInfo))

	// 当前任务为map任务时，需要设置输入的文件
	if task.Phase == MapPhase {
		task.FileName = m.files[taskSeq]
	}

	return task
}

// 注册已分配worker的任务，填充worker相关的信息和修改运行状态
func (m *Master) registerTask(args *AskTaskArgs, task *Task){
	m.mu.Lock()
	defer m.mu.Unlock()

	if task.Phase != m.taskPhase {
		panic("register task phase neq")
	}

	m.taskStatInfo[task.Seq].Status = TaskStatusRunning
	m.taskStatInfo[task.Seq].WorkerId = args.WorkerId
	m.taskStatInfo[task.Seq].StartTime = time.Now()
}