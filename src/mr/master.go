package mr

import(
	"log"
	"net"
	"os"
	"io/ioutil"
	"net/rpc"
	"net/http"
	"sync"
	"time"
	//"fmt"
)
//任务状态
const (
	WAITING =  0
	WORKING = 1
	FINISHED = 2 
)


type Master struct {
	// Your definitions here.
	filesCnt              int
	nReduce           int 
	files                    [] string  //所有的文件名称
	mapStatus [] int //0 - 等待中 1 - 处理中 2 - 成功
	mapDone bool 
	reduceStatus [] int //0 - 等待中 1 - 处理中 2 - 成功
	reduceDone bool
}
type Monitor struct{
	taskType int
	seq int
	startTime time.Time
	overTime string
}
var moitorTaskList = make([] Monitor,0)

var lockSelect sync.RWMutex
var lockMonitor sync.RWMutex

func (m * Master) moitor() {
	for true {
		lockMonitor.Lock()
		var t [] Monitor
		for i,j  :=  range moitorTaskList  {
			if time.Now().After(j.startTime) {
				if j.taskType == MAP_TASK {
					if m.mapStatus[j.seq] == WORKING {
						m.mapStatus[j.seq] = WAITING
					}else{
						t = append(moitorTaskList[:i],moitorTaskList[i+1:]...)
					}
				}else{
					if m.reduceStatus[j.seq] == WORKING {
						m.reduceStatus[j.seq] = WAITING
					}else{
						t  = append(moitorTaskList[:i],moitorTaskList[i+1:]...)
					}
				}
			}
		}
		moitorTaskList = t
		lockMonitor.Unlock()
		time.Sleep(3*time.Second)
	}
}

// 加锁 ？？！
func selectTask  (m * Master) (int,int)  {
	
	if !m.mapDone  {
		for i,j := range m.mapStatus {
			if j == WAITING{
				m.mapStatus[i] = WORKING
				return MAP_TASK, i
			}
		}
		for _,j := range m.mapStatus {
			if j != FINISHED{
				return 2, 0
			}
		}
		m.mapDone = true
		//fmt.Println("All Map Task is Done")
		
	} else if  m.mapDone  &&  !m.reduceDone {

		for i,j := range m.reduceStatus {
			if j == WAITING{
				m.reduceStatus[i] = WORKING
				return REDUCE_TASK, i

			}
		}
		for _,j := range m.reduceStatus {
			if j != FINISHED{
				return 2, 0
			}
		}
		m.reduceDone = true
	//	fmt.Println("All Reduce Task is Done")
	}else{
		//fmt.Println("All task were done！" )
		os.Exit(0)
		return 2,0
	}
	return 2,0
}
// Your code here -- RPC handlers for the worker to call.
// 处理worker的rpc请求
func (m *Master) HandlerRequest(request  * Request, response   *Response) error{
	

	m.doReq(request, response)
	return nil
} 
//定义一个队列,每10s处理一次


func (m *Master) doReq(request  *Request, response * Response){
	//添加计时器, 如果这个没有在给定时间内答复已完成,则将该任务修改成Wating状态
	switch request.RequestType {
		case ASK_FOR_TASK:
			lockSelect.Lock()
			response.TaskType, response.Seq = selectTask(m)
			defer lockSelect.Unlock()
			response.NReduce = m.nReduce
			//fmt.Printf("任务 %v  Seq %v \n",response.TaskType, response.Seq )
			if response.TaskType == MAP_TASK {
				response.Filename =  m.files[response.Seq]
				filename := m.files[response.Seq]
				file, err := os.Open(response.Filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				response.FileContent = string(content)
				
			}
			//添加到监控队列中
			if response.TaskType != 2{
				m := Monitor{}
				m.taskType = response.TaskType
				m.seq  = response.Seq
				m.startTime = time.Now()
				lockMonitor.Lock()
				moitorTaskList = append(moitorTaskList,m)
				lockMonitor.Unlock()
			}
		case NOTICE:
			//fmt.Printf("完成 %v 任务的Seq：%v\n" , request.TaskType,request.Seq)
			//修改序列号
			if request.TaskType == MAP_TASK {
				if  m.mapStatus[request.Seq] == WORKING {// 防止过慢的误修改
					m.mapStatus[request.Seq] = FINISHED
				}
			}else if  request.TaskType == REDUCE_TASK{
				if  m.reduceStatus[request.Seq] == WORKING{ // 防止过慢的误修改
				    m.reduceStatus[request.Seq] = FINISHED
				}
			}
	}
	
}
// start a thread that listens for RPCs from worker.go

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
//所有任务结束后，调用该方法将结果写入一个文件中
//
func (m *Master) Done() bool {
	ret := false 

	// Your code here.
	if m.mapDone && m.reduceDone {
		ret = true
	}

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.   nReduce是要使用的reduce任务的数量

func MakeMaster(files []string, nReduce int) *Master {
	//Master对象
	m := Master{}
	// Your code here.
	//读取到所有的文件名称m
	m.files = files
	len := len(files)
	//初始化
	m.nReduce = nReduce
	m.mapStatus = make([] int, len )
	m.reduceStatus= make([] int, nReduce)
	m.filesCnt = len
	m.server()
	go m.moitor()
	return &m
}
