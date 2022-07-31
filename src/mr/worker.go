package mr

import (
	"encoding/json"
	"fmt"
	"golang.org/x/sys/unix"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

type worker struct {
	id      int64
	reduceN int
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var w = worker{int64(unix.Getpid()), 0}

func (wk *worker) doMap(fileNames []string, mapf func(string, string) []KeyValue) {
	fmt.Printf("Map任务开始, fileNames = %v\n", fileNames)
	intermediates := make([][]KeyValue, wk.reduceN)
	for i := 0; i < len(intermediates); i++ {
		intermediates[i] = make([]KeyValue, 0)
	}
	for _, fileName := range fileNames {
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("[%v] 不能打开文件%v", wk, fileName)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("[%v] 不能读文件%v", wk, fileName)
		}
		file.Close()
		kva := mapf(fileName, string(content))
		for _, kv := range kva {
			rIndex := ihash(kv.Key) % wk.reduceN
			intermediates[rIndex] = append(intermediates[rIndex], kv)
		}
	}
	for rIndex, intermediate := range intermediates {
		mapOutputName := fmt.Sprintf("map_%v_%v", w.id, rIndex)
		file, err := os.OpenFile(mapOutputName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0777)
		if err != nil {
			log.Fatalf("[%v] 文件打开/编辑失败 fileName = %v", wk, mapOutputName)
		}
		enc := json.NewEncoder(file)
		for _, kv := range intermediate {
			enc.Encode(&kv)
		}
		file.Close()
	}
	fmt.Printf("Map任务完成, fileNames = %v\n", fileNames)
	wk.CallDoneTask()
}

func (wk *worker) doReduce(fileNames []string, reducef func(string, []string) string) {
	fmt.Printf("开始处理reduce任务 fileNames=%v\n", fileNames)
	var kva []KeyValue
	if len(fileNames) == 0 {
		log.Fatalf("reduce处理文件列表为空")
	}
	for _, fileName := range fileNames {
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("不能打开文件%v\n", fileName)
		}
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
	reduceTaskIndex := strings.Split(fileNames[0], "_")[2]
	oname := fmt.Sprintf("mr-out-%v", reduceTaskIndex)
	tempFile, err := ioutil.TempFile("", "reduce-inter-*")
	if err != nil {
		log.Fatalf("创建临时文件错误 err = %v", err.Error())
	}
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
		fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	//判断目标reduce文件是否存在，原子操作
	_, err = os.Stat(oname)
	if err == nil {
		//目标已经文件存在了，删除临时文件
		err = os.Remove(tempFile.Name())
		if err != nil {
			log.Fatalf("删除reduce输出临时文件失败fileName = %v", tempFile.Name())
		}
	} else {
		if os.IsNotExist(err) {
			//reduce目标文件不存在，进行改名操作
			fmt.Printf("reduce开始对中间文件进行更名fileName = %v", tempFile.Name())
			err = os.Rename(tempFile.Name(), oname)
			if err != nil {
				log.Fatalf("修改reduce临时输出文件名称失败tempName = %v, outputName = %v, err = %v", tempFile.Name(), oname, err)
			}
		} else {
			log.Fatalf("判断文件状态错误fileName = %v", oname)
		}
	}
	fmt.Println("reduce任务处理完成")
	wk.CallDoneTask()
}

//
// 访问master获取任务
//
func (wk *worker) CallGetTask() (taskType string, fileNames []string) {
	args := &GetTaskArgs{WorkerId: wk.id}
	reply := &GetTaskReply{}
	ok := call("Coordinator.GetTask", args, reply)
	if ok {
		wk.reduceN = reply.ReduceN
		return reply.TaskType, reply.FileNames
	} else {
		//当master无法访问的时候，代表任务已经终止，结束自己。
		log.Fatalf("rpc请求失败，进程结束")
	}
	return "", nil
}

func (wk *worker) CallDoneTask() {
	args := &DoneTaskArgs{WorkerId: wk.id}
	reply := &DoneTaskReply{}
	ok := call("Coordinator.DoneTask", args, reply)
	if !ok {
		//当master无法访问的时候，代表任务已经终止，结束自己。
		log.Fatalf("rpc请求失败，进程结束")
	}
	return
}

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
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	//第一次获取任务，和server自身信息
	for true {
		taskType, fileNames := w.CallGetTask()
		fmt.Printf("woker%v获取到任务type=%v file=%v\n", w.id, taskType, fileNames)
		//如果fileNames不为空说明有任务
		if len(fileNames) > 0 {
			if taskType == "map" {
				w.doMap(fileNames, mapf)
			} else if taskType == "reduce" {
				w.doReduce(fileNames, reducef)
			} else {
				log.Printf("%v %v", w, "任务类型错误")
			}
		}
		//lab1的任务说是最多1s内能完成，睡1s也能在心跳期内完成刷新
		time.Sleep(1 * time.Second)
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
