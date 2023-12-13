package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "encoding/json"
import "os"
import "strconv"
import "io/ioutil"
import "sort"
//import "time"

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


//
// main/mrworker.go calls this function.
// 传入两个匿名函数，一个是map函数，一个是reduce函数
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// 死循环向master请求任务
	for true{
		req := Request{}
		req.RequestType = ASK_FOR_TASK
		resp,exit  := CallMasterForTask(req)
		if  !exit  { 
			//fmt.Println("master 失联")
			return
		}
		switch resp.TaskType {
			case MAP_TASK: 
			// Map任务，负责将一个文件的K-V对放入对应的Reduce桶
				var  m =  make(map[int][]KeyValue, resp.NReduce)
				var mapRes  []  KeyValue = mapf(resp.Filename, resp.FileContent)
				for _,kv := range mapRes {
					//fmt.Println(kv.Key)
					key := ihash(kv.Key) % resp.NReduce
					//fmt.Printf("K：",key)
					if m[key] == nil {
						m[key] = make([]KeyValue,0)
					}
					m[key] = append(m[key],kv)
				}
				//遍历每一个map
				var  fatherPath  string = "mr-tmp/reduce-"
				for k,v := range m {
					os.MkdirAll( fatherPath + strconv.Itoa(k), os.ModePerm)
					filename :=fatherPath + strconv.Itoa(k) +  "/mr-" + strconv.Itoa(resp.Seq) +"-"+ strconv.Itoa(k) + ".json"
					file,_:= os.Create(filename)
					// if err != nil{
					// 	fmt.Printf("Task Map Seq %v Create fail: %v \n",resp.Seq, err)
					// }
					//fmt.Println(v)
					enc:= json.NewEncoder(file)
					for _,kv := range  v {
						enc.Encode(&kv)
					}
					defer file.Close()
				}
				//执行完成，通知master
		
				req:=Request {}
				req.RequestType = NOTICE
				req.Seq = resp.Seq
				req.TaskType = MAP_TASK
				CallMasterForTask(req)
			case REDUCE_TASK: // Reduce 任务
				//读取文件
				dir := "mr-tmp/reduce-" + strconv.Itoa(resp.Seq)

				files,_ := ioutil.ReadDir(dir)
				// if err != nil {
				// 	fmt.Printf("Task Reduce Seq %v read fail: %v \n",resp.Seq, err)
				// }
				kva := make([] KeyValue,0)
				
				for _,fileInfo := range files {
					file,_ := os.Open(dir + "/" +fileInfo.Name())
					//if err != nil {
					//	fmt.Printf("Task Reduce Seq %v open fail: %v \n",resp.Seq, err)
					//}
					dec := json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
						  break
						}
						kva = append(kva, kv)
					  }
				}
				sort.Sort(ByKey(kva))
				oname := "mr-out-" + strconv.Itoa(resp.Seq)

				ofile, _ := os.Create(oname)	
				//if err != nil {
				//	fmt.Printf("Task Reduce Seq %v create fail: %v \n",resp.Seq, err)
				//}
				//
				// call Reduce on each distinct key in kva[],
				// and print the result to mr-out-X.
				//
				i := 0
				for i < len(kva) {
					j := i + 1
					for j < len(kva) && kva[j].Key == kva[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, kva[k].Value)
					}
					output := reducef(kva[i].Key, values)

					// this is the correct format for each line of Reduce output.
					fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

					i = j
				}

				ofile.Close()
				//删除对应的reduce任务文件夹
				os.RemoveAll(dir)
				//执行完成，通知master
				req:=Request {}
				req.RequestType = NOTICE
				req.Seq = resp.Seq
				req.TaskType = REDUCE_TASK
				CallMasterForTask(req)
			case 2:	 // 保留，无任务
				//time.Sleep(1)
				//fmt.Println("暂无任务")
		}
	}
}

func CallMasterForTask(req Request) (Response,bool) {
		response  := Response{}
		if !call("Master.HandlerRequest",&req, &response ){
			return response,false
		}
		//fmt.Printf("%v 任务，Seq：%v\n",req.TaskType,req.Seq)
		return response,true
}
//
// send an RPC request to the master, wait for the response.

// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, request interface{}, response interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	

	err = c.Call(rpcname, request, response)
	if err == nil {
		return true
	}

	return false
}
