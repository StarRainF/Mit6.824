package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"

type Task struct{
	FileName string
	//State	int // 0 start 1 running 2 finish
	MachineId int
	State int
	Runtime int
}

type Coordinator struct {
	// Your definitions here.
	State int // 0 Map  1 Reduce
	NMap int //最大并行map个数
	NReduce int //最大并行reduce个数

	MachineNum int  //用来给机器分配machine id从1开始
	MapTask map[int]* Task
	ReduceTask map[int]*Task
	Mu sync.Mutex //只能有一个worker访问
}

//更新所有的task的Runtime+1，如果达到10则仍为该机器挂了，需要一个新机器去完成
func(c *Coordinator)TimeTick() {
	c.Mu.Lock()
	if(c.State == 0){
		for tasknumber, task := range(c.MapTask){
			if(task.State == 1){
				c.MapTask[tasknumber].Runtime = c.MapTask[tasknumber].Runtime + 1
				if(c.MapTask[tasknumber].Runtime >= 10){
					c.MapTask[tasknumber].State = 0;
				}
			}
		}
	}else if(c.State == 1){
		for tasknumber , task := range(c.MapTask){
			if(task.State == 1){
				c.ReduceTask[tasknumber].Runtime = c.ReduceTask[tasknumber].Runtime + 1
				if(c.ReduceTask[tasknumber].Runtime >= 10){
					c.ReduceTask[tasknumber].State = 0
				}
			}
		}
	}
	c.Mu.Unlock()
}

func(c *Coordinator)UpdateCoordinatorState() {
	for _, task := range(c.MapTask){
		if(task.State == 0 || task.State == 1){//start or running
			c.State = 0 //map阶段
			return
		}
	}
	for _, task := range(c.ReduceTask){
		if(task.State == 0 || task.State == 1){
			c.State = 1 //reduce阶段
			return
		}
	}
	c.State = 2
}
// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//请求任务  State 0:map 1: reduce 2 空转 3 完成
func (c *Coordinator) AskTask (args *AskArgs, reply *AskReply) error {
	c.Mu.Lock()

	reply.State = 2
	reply.NMap = c.NMap
	reply.NReduce = c.NReduce
	if(args.MachineId == 0){//分配机器号
		c.MachineNum = c.MachineNum + 1
		reply.MachineId = c.MachineNum
	}else{
		reply.MachineId = args.MachineId
	}
	if(c.State == 0){//如果是map
		for taskNumber, task := range(c.MapTask){
			if(task.State == 0){
			
				reply.State = 0
				reply.FileName = task.FileName
				reply.TaskNumber = taskNumber
				c.MapTask[taskNumber].State = 1//正在处理
				break
			}
		}
	}else if (c.State == 1) {// reduce
		for taskNumber , task := range(c.ReduceTask){
			if(task.State == 0){
			
				reply.State = 1
				reply.TaskNumber = taskNumber
				c.ReduceTask[taskNumber] .State = 1
				break
			}
		}
	}else{
		
	}
	c.Mu.Unlock()
	return nil;

}
/*
*Coordinator收到FinishTask请求时更新Coordinator状态
*当Coordinator状态为2，mapreduce全部完成时，reply的State置为0终止Worker
*/
func (c *Coordinator) FinishTask(args FinishArgs, reply *FinishReply) error {
	c.Mu.Lock()
	reply.State = 0
	if(args.State == 0){
		c.MapTask[args.TaskNumber].State = 2//完成
		c.UpdateCoordinatorState()
	}else if(args.State == 1){
		c.ReduceTask[args.TaskNumber].State = 2
		c.UpdateCoordinatorState()
		if(c.State == 2){//所有任务都完成
			reply.State = 1
		}
	}
	c.Mu.Unlock()
	return nil;
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
	

	// Your code here.
	var ret bool;
	if(c.State == 2){
		ret = true
	}else{
		ret = false
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mapTask := make(map[int]*Task)
	reduceTask := make(map[int]*Task)
	for i, filename := range(files){
		mapTask[i] = &Task{FileName: filename, MachineId: 0 ,State:0,
		Runtime: 0}
	}
	for j:= 0; j < nReduce; j++{
		reduceTask[j] = &Task{FileName: "",MachineId: 0,State: 0,Runtime: 0}
	}
	c := Coordinator{State: 0, NMap: len(files), NReduce: nReduce , MapTask: mapTask, 
		ReduceTask: reduceTask , MachineNum: 0 ,Mu: sync.Mutex{}}
	c.server()
	return &c
}
