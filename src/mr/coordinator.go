package mr

import (
	"fmt"
	"log"
	"mit6.824-lab/constant"
	"mit6.824-lab/util/collections_helper"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

var coordinator *Coordinator

type server struct {
	status          int       //0:空闲 1:执行任务中 2:超时或者宕机
	taskType        string    // 运行的任务类型 ,"map" "reduce" (只有status=1时才成立)
	mapTaskDone     []string  //已经完成的map任务
	mapTaskDoing    []string  //正在执行的map任务
	reduceTaskDone  []int     //已经完成的reduce任务
	reduceTaskDoing []int     //正在执行的reduce任务
	lastHeartBeat   time.Time //上一次的心跳时期
}

func (s *server) heartCheck() {
	go func() {
		for true {
			if time.Now().Sub(s.lastHeartBeat) > constant.WorkerTimeOut {
				//server超过10s没有延续心跳，判为宕机，结束该server的监听
				fmt.Printf("检测到worker-%v失去心跳，转移任务，视为宕机", s)
				coordinator.Lock()
				coordinator.unCommitMapTasks = append(coordinator.unCommitMapTasks, append(s.mapTaskDone, s.mapTaskDoing...)...)
				s.mapTaskDoing, s.mapTaskDone = []string{}, []string{}
				coordinator.unCommitReduceTasks = append(coordinator.unCommitReduceTasks, s.reduceTaskDoing...)
				s.reduceTaskDoing = []int{}
				coordinator.Unlock()
				s.status = 2
				break
			}
			time.Sleep(constant.HeartDuration)
		}
	}()
}

type Coordinator struct {
	sync.RWMutex
	mapN                int               //map任务数目(文件数目)
	reduceN             int               //reduce任务的数目
	status              int               //0:map阶段 1:reduce阶段 2:完成
	servers             map[int64]*server // server列表，每个server有一个全局唯一的id
	mapOutputLocations  []int64
	unCommitMapTasks    []string //还未提交的map任务
	unCommitReduceTasks []int    //还未提交reduce任务
}

func (c *Coordinator) GetReduceTaskFileNames(reduceTask int) []string {
	var res []string
	for _, location := range c.mapOutputLocations {
		res = append(res, fmt.Sprintf("map_%v_%v", location, reduceTask))
	}
	return res
}

func (c *Coordinator) DoneTask(args *DoneTaskArgs, reply *DoneTaskReply) error {
	c.Lock()
	//如果这个server还没注册过,就报错
	if _, ok := c.servers[args.WorkerId]; !ok {
		log.Fatalf("未注册的server报道完成任务 workerId = %v", args.WorkerId)
	}
	s := c.servers[args.WorkerId]
	s.lastHeartBeat = time.Now()
	//如果之前宕过机，说明是超时了，
	if s.status == 2 {
		s.status = 0
		return nil
	}
	if s.taskType == "map" {
		s.mapTaskDone = append(s.mapTaskDone, s.mapTaskDoing...)
		s.mapTaskDoing = []string{}
	} else if s.taskType == "reduce" {
		s.reduceTaskDone = append(s.reduceTaskDone, s.reduceTaskDoing...)
		s.reduceTaskDoing = []int{}
	} else {
		log.Fatalf("worker完成任务-任务类型错误 worker = %+v", s)
	}
	s.status = 0

	//检测是否map阶段任务或者reduce阶段任务已经完成
	mapTaskDone, reduceTaskDone := &collections_helper.Set[string]{}, &collections_helper.Set[int]{}
	for _, ser := range c.servers {
		if ser.status != 2 {
			mapTaskDone.Add(ser.mapTaskDone...)
		}
		//计算reduce server已经宕机了，但是它的输出文件是存在gfs里的依然可以读取
		reduceTaskDone.Add(ser.reduceTaskDone...)
	}
	if c.status == 0 && mapTaskDone.Size() == c.mapN {
		c.status = 1
		for workerId, ser := range c.servers {
			if len(ser.mapTaskDone) > 0 && ser.status != 2 {
				c.mapOutputLocations = append(c.mapOutputLocations, workerId)
			}
		}

	}
	if c.status == 1 && reduceTaskDone.Size() == c.reduceN {
		c.status = 2
	}
	c.Unlock()
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.Lock()
	//如果这个server还没注册过，就先注册
	if _, ok := c.servers[args.WorkerId]; !ok {
		c.servers[args.WorkerId] = &server{
			mapTaskDone:     make([]string, 0),
			mapTaskDoing:    make([]string, 0),
			reduceTaskDone:  make([]int, 0),
			reduceTaskDoing: make([]int, 0),
			lastHeartBeat:   time.Now(),
		}
		//开启心跳监测
		c.servers[args.WorkerId].heartCheck()
	}
	s := c.servers[args.WorkerId]
	s.lastHeartBeat = time.Now()

	if c.status == 0 && len(c.unCommitMapTasks) > 0 {
		//分配map任务
		fileName := c.unCommitMapTasks[0]
		c.unCommitMapTasks = c.unCommitMapTasks[1:]
		s.status = 1
		s.taskType = "map"
		s.mapTaskDoing = append(s.mapTaskDoing, fileName)
		reply.TaskType = "map"
		reply.ReduceN = c.reduceN
		reply.FileNames = []string{fileName}
	} else if c.status == 1 && len(c.unCommitReduceTasks) > 0 {
		//分配reduce任务
		reduceTask := c.unCommitReduceTasks[0]
		c.unCommitReduceTasks = c.unCommitReduceTasks[1:]
		s.status = 1
		s.taskType = "reduce"
		s.reduceTaskDoing = append(s.reduceTaskDoing, reduceTask)
		reply.TaskType = "reduce"
		reply.ReduceN = c.reduceN
		reply.FileNames = c.GetReduceTaskFileNames(reduceTask)
	} else {
		//目前没有要分配的任务
		s.status = 0
		reply.TaskType = ""
		reply.ReduceN = c.reduceN
		reply.FileNames = []string{}
	}
	c.Unlock()
	return nil
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	return c.status == 2
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	coordinator = &Coordinator{
		status:              0,
		mapN:                len(files),
		reduceN:             nReduce,
		servers:             make(map[int64]*server, 0),
		unCommitMapTasks:    make([]string, 0),
		unCommitReduceTasks: make([]int, 0),
	}
	coordinator.unCommitMapTasks = append(coordinator.unCommitMapTasks, files...)
	for i := 0; i < nReduce; i++ {
		coordinator.unCommitReduceTasks = append(coordinator.unCommitReduceTasks, i)
	}
	coordinator.server()
	return coordinator
}
