package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "strconv"
import "sort"
import "encoding/json"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type Bykey []KeyValue

func(a Bykey) Len() int    {return len(a) }
func(a Bykey) Swap(i , j int) {a[i] , a[j] = a[j] , a[i]}
func(a Bykey) Less(i,j int) bool {return a[i].Key < a[j].Key} 
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
	machineId := 0
	for{
		args := AskArgs{MachineId: machineId}
		reply := AskReply{}
		CallAskTask(&args, &reply)
		machineId = reply.MachineId
		taskNumber := reply.TaskNumber
		//根据reply.State确定做map还是reduce还是循环等待或退出
		if(reply.State == 0){
			//fmt.PrintLn("正在执行Map操作",reply.FileName)
			file, err := os.Open(reply.FileName)
			if err != nil{
				log.Fatalf("cnanot open mapTask %v",reply.FileName)
			}
			content ,err := ioutil.ReadAll(file)
			if err != nil{
				log.Fatalf("cnanot read%v",reply.FileName)
			}
			file.Close()
			kva := mapf(reply.FileName,string(content))
			//写入mr-taskNumber-y文件中
			WriteMapOutput(kva, taskNumber , reply.NReduce)
		}else if(reply.State == 1){
			//读取json格式的mapoutput文件
			intermediate := []KeyValue{}
			nmap := reply.NMap
			for i :=0; i < nmap; i++{
				mapOutFilename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(taskNumber)
				inputFile, err := os.OpenFile(mapOutFilename,os.O_RDONLY,0777)
				if err != nil{
					log.Fatalf("cannot open reduceTask %v",reply.FileName)
				}
				dec := json.NewDecoder(inputFile)
				for{
					var kv []KeyValue
					if err := dec.Decode(&kv); err!=nil{
						break
					}
					intermediate = append(intermediate,kv...)
				}
			}
			sort.Sort(Bykey(intermediate))
			oFilename := "mr-out-" + strconv.Itoa(taskNumber)
			tmpReduceOutFIle, err := ioutil.TempFile("","mr-reduce-*")
			if err != nil{
				log.Fatalf("cannot open %v",reply.FileName)
			}
			i := 0
			for i < len(intermediate){
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key{
					j++
				}
				values := []string{}
				//合并value进字符串数组
				for k:= i ; k < j ; k++{
					values = append(values,intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key,values)
				fmt.Fprintf(tmpReduceOutFIle,"%v %v\n",intermediate[i].Key , output)
				i = j
			}
			tmpReduceOutFIle.Close()
			os.Rename(tmpReduceOutFIle.Name(),oFilename)

		}else if(reply.State == 2){
			continue
		}else if(reply.State == 3){
			break
		}
		finishargs := FinishArgs{State: reply.State , TaskNumber: taskNumber}
		finishreply := FinishReply{}
		CallFinishTask(&finishargs,&finishreply)
		if(finishreply.State == 1){
			break
		}
	}
	
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}
func CallAskTask(args *AskArgs, reply *AskReply){
	call("Coordinator.AskTask", &args,&reply)
}

func CallFinishTask(args *FinishArgs, reply *FinishReply){
	call("Coordinator.FinishTask", &args,&reply)
}

func WriteMapOutput(kva []KeyValue , taskNumber int , nReduce int) bool{
	buf := make([][]KeyValue,nReduce)
	for _, key_val := range(kva) {
		no := (ihash(key_val.Key)) % nReduce
		buf[no] = append(buf[no], key_val)
	}
	for no, key_val_nums := range(buf){
		mapOutFilename := "mr-" + strconv.Itoa(taskNumber) + "-" +strconv.Itoa(no)
		tmpMapOutFile , error := ioutil.TempFile("","mr-map-*")
		if error != nil{
			log.Fatalf("cannot open tmpMapOutFile")
		}
		enc := json.NewEncoder(tmpMapOutFile)
		err := enc.Encode(key_val_nums)
		if err != nil{
			return false
		}
		tmpMapOutFile.Close()
		os.Rename(tmpMapOutFile.Name() , mapOutFilename)
	}
	return true
}
//
// example function to show how to make an RPC call to the coordinator.
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
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
