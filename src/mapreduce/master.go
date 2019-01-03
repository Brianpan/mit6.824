package mapreduce

import "container/list"
import "fmt"
import "time"

type WorkerInfo struct {
	address string
	// You can add definitions here.
	status bool
	jtype  string
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Register WorkerInfo
	go mr.ReceiveWorker()
	// sleep a little bit while for registering workers
	time.Sleep(100 * time.Millisecond)
	// do map process

	return mr.KillWorkers()
}

// Register worker
func (mr *MapReduce) ReceiveWorker() {
	for {
		newWorkerAddress := <-mr.registerChannel
		workerInfo := new(WorkerInfo)
		workerInfo.address = newWorkerAddress
		workerInfo.status = false
		workerInfo.jtype = ""
		mr.mu.Lock()
		mr.Workers[newWorkerAddress] = workerInfo
		// available worker
		mr.AvailableWorkers.PushBack(workerInfo)
		mr.mu.Unlock()
	}
}

// Parallel
func (mr *MapReduce) ParallelMap(mid int) *WorkerInfo {

}
