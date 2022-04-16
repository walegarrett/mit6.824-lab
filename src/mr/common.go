package mr

import (
	"fmt"
	"log"
)

// 任务的阶段-map阶段还是reduce阶段
type TaskPhase int

const (
	MapPhase TaskPhase = 0
	ReducePhase TaskPhase = 1
)

type Task struct {
	FileName string // 当前任务处理的文件名，只有map任务有这个属性
	NReduce int // 当前job的reducer数量
	NMaps int // 当前job的mapper数量
	Seq int // 任务序号，mapper任务和reducer任务不是累加的，而是共用一些序号
	Phase TaskPhase 
	Alive bool // 
}

// 用于debug模式
const Debug = false

func DPrintf(format string, v ...interface{}){
	if Debug {
		log.Printf(format+"\n", v...)
	}
}

// 中间文件名称
func reduceName(mapIdx, reduceIdx int) string {
	return fmt.Sprintf("mr-%d-%d", mapIdx, reduceIdx)
}

// 每个reducer产生的结果文件名称
func mergeName(reduceIdx int) string {
	return fmt.Sprintf("mr-out-%d", reduceIdx)
}