package mapreduce

import (
	"fmt"
	"strconv"
	"sync"
)

type AtomicTasks struct {
	sync.Mutex
	nTasks int
}

func (a *AtomicTasks) PopTask() (r int) {
	a.Lock()
	a.nTasks--
	r = a.nTasks

	a.Unlock()

	return r
}

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//

func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)

	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	wg := new(sync.WaitGroup)
	atomicTasks := AtomicTasks{nTasks: ntasks}
	workerChan := make(chan string)

	go func(workerChan chan string, atomicTasks *AtomicTasks, wg *sync.WaitGroup) {
		chan_closed := false
		for worker := range workerChan {
			task := atomicTasks.PopTask()
			fmt.Println("POPPED TASK" + strconv.Itoa(task))
			if task < 0 {
				fmt.Println("WORKER CLOSING" + worker)
				fmt.Println("CLOSING CHAN")
				if !chan_closed {
					chan_closed = true
					close(registerChan)
				}
			} else {
				args := DoTaskArgs{
					JobName:       jobName,
					File:          mapFiles[task],
					Phase:         phase,
					TaskNumber:    task,
					NumOtherPhase: n_other,
				}
				go func(wg *sync.WaitGroup, worker string, args DoTaskArgs, workerChan chan string) {
					wg.Add(1)
					result := call(worker, "Worker.DoTask", args, nil)
					fmt.Println(result)

					workerChan <- worker

					wg.Done()
				}(wg, worker, args, workerChan)
			}
		}
	}(workerChan, &atomicTasks, wg)
	for rpcAddress := range registerChan {
		workerChan <- rpcAddress
	}
	wg.Wait()
	close(workerChan)

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	fmt.Printf("Schedule: %v phase done\n", phase)
}
