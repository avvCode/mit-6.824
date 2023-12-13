package mr

//
// RPC definitions.
//
// remember to capitalize all names.
// 参数名称需要大写，否则序列化反序列化报错
// 

import "os"
import "strconv"

//任务类型
const(
	MAP_TASK = 0 //map任务
	REDUCE_TASK = 1 // reduce任务
)
//请求类型
const(
	ASK_FOR_TASK = 0 //请求任务
	NOTICE = 1 // 通知
)

//请求
type Request struct {
	RequestType int // 0 - 请求任务 1- 通知
	Seq int // 完成的任务序号 
	TaskType int // 完成的任务类型
}
//回复
type Response struct {
	TaskType int // 0-Map 1-Reduce
	Seq int //任务序号
	Filename string // 文件名称
	FileContent string // 文件内容
	NReduce int //
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
