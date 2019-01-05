package mapreduce

import "container/list"
import "fmt"
import "time"

type WorkerInfo struct {
	address string
	// You can add definitions here.
	status int
	jtype  string
	errCount int
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
	mr.ParallelMap()
	// do reduce process
	mr.ParallelReduce()

	return mr.KillWorkers()
}

// Register worker
func (mr *MapReduce) ReceiveWorker() {
	for {
		newWorkerAddress := <-mr.registerChannel
		workerInfo := WorkerInfo{}
		workerInfo.address = newWorkerAddress
		workerInfo.status = 0
		workerInfo.jtype = ""
		workerInfo.errCount = 0
		mr.mu.Lock()
		mr.Workers[newWorkerAddress] = workerInfo
		// available worker
		mr.AvailableWorkers.PushBack(&workerInfo)
		mr.mu.Unlock()
	}
}

// Parallel
func (mr *MapReduce) ParallelMap() {
	jid := 0
	for mr.MapCounter < mr.nMap {
		for mr.AvailableWorkers.Len() > 0 && jid < mr.nMap && mr.MapCounter < mr.nMap {
			mr.mu.Lock()
			ele :=  mr.AvailableWorkers.Front()
			mr.AvailableWorkers.Remove(ele)
			worker := ele.Value.(*WorkerInfo)
			mr.mu.Unlock()
			go mr.RunMap(jid, worker)
			jid += 1
		}

		// any error jobs to handle
		for mr.AvailableWorkers.Len() > 0 && mr.ErrorJobs.Len() > 0 {
			mr.mu.Lock()
			ele := mr.ErrorJobs.Front()
			mr.ErrorJobs.Remove(ele)
			mr.mu.Unlock()
			ejid := ele.Value.(int)
			mr.mu.Lock()
			ele =  mr.AvailableWorkers.Front()
			mr.AvailableWorkers.Remove(ele)
			worker := ele.Value.(*WorkerInfo)
			mr.mu.Unlock()
			go mr.RunMap(ejid, worker)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
// Run each map
func (mr *MapReduce) RunMap(jid int, worker *WorkerInfo) {
	args := &DoJobArgs{}
	args.File = mr.file
	args.Operation = Map
	args.JobNumber = jid
	args.NumOtherPhase = mr.nReduce

	var reply DoJobReply
	ok := call(worker.address, "Worker.DoJob", args, &reply)
	mr.mu.Lock()
	mr.MapStatus[jid] = Running
	mr.mu.Unlock()
	for ok == false || reply.OK == false {
		// worker dead
		if worker.errCount >= ErrorBound {
			mr.mu.Lock()
			worker.status = -1
			mr.ErrorJobs.PushBack(jid)
			mr.mu.Unlock()
			return
		}
		worker.errCount += 1
		ok = call(worker.address, "Worker.DoJob", args, &reply)
	}
	
	mr.mu.Lock()
	mr.AvailableWorkers.PushBack(worker)
	mr.MapCounter += 1
	mr.MapStatus[jid] = Finished
	mr.mu.Unlock()
}

func (mr *MapReduce) ParallelReduce() {
	jid := 0
	for mr.ReduceCounter < mr.nReduce {
		for mr.AvailableWorkers.Len() > 0 && jid < mr.nReduce && mr.ReduceCounter < mr.nReduce {
			mr.mu.Lock()
			ele :=  mr.AvailableWorkers.Front()
			mr.AvailableWorkers.Remove(ele)
			worker := ele.Value.(*WorkerInfo)
			mr.mu.Unlock()
			go mr.RunReduce(jid, worker)
			jid += 1
		}
		// any error jobs to handle
		for mr.AvailableWorkers.Len() > 0 && mr.ErrorJobs.Len() > 0 {
			mr.mu.Lock()
			ele := mr.ErrorJobs.Front()
			mr.ErrorJobs.Remove(ele)
			mr.mu.Unlock()
			ejid := ele.Value.(int)
			mr.mu.Lock()
			ele =  mr.AvailableWorkers.Front()
			mr.AvailableWorkers.Remove(ele)
			worker := ele.Value.(*WorkerInfo)
			mr.mu.Unlock()
			go mr.RunReduce(ejid, worker)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (mr *MapReduce) RunReduce(jid int, worker *WorkerInfo) {
	args := &DoJobArgs{}
	args.File = mr.file
	args.Operation = Reduce
	args.JobNumber = jid
	args.NumOtherPhase = mr.nMap

	var reply DoJobReply

	ok := call(worker.address, "Worker.DoJob", args, &reply)
	mr.mu.Lock()
	mr.ReduceStatus[jid] = Running
	mr.mu.Unlock()
	for ok == false || reply.OK == false {
		// worker dead
		if worker.errCount >= ErrorBound {
			mr.mu.Lock()
			mr.ErrorJobs.PushBack(jid)
			worker.status = -1
			mr.mu.Unlock()
			return
		}
		worker.errCount += 1
		ok = call(worker.address, "Worker.DoJob", args, &reply)
	}
	mr.mu.Lock()
	mr.AvailableWorkers.PushBack(worker)
	mr.ReduceCounter += 1
	mr.ReduceStatus[jid] = Finished
	mr.mu.Unlock()
}