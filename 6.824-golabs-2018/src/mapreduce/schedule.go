package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
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

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	var wg sync.WaitGroup

	// concurrent tasks
	for i := 0; i < ntasks; i++ {
		wg.Add(1)
		go func(taskID int) {
			// Decrement the counter when the goroutine completes.
			defer wg.Done()

			var taskArgs DoTaskArgs
			switch phase {
			case mapPhase:
				taskArgs = DoTaskArgs{JobName: jobName, File: mapFiles[taskID], Phase: phase, TaskNumber: taskID, NumOtherPhase: n_other}
			case reducePhase:
				taskArgs = DoTaskArgs{JobName: jobName, File: "", Phase: phase, TaskNumber: taskID, NumOtherPhase: n_other}
			}

			// loop forever till task is completed
			for {
				// concurrent workers
				worker := <-registerChan
				taskDone := call(worker, "Worker.DoTask", taskArgs, nil)
				// task is completed
				if taskDone {
					fmt.Printf("job %d done by worker %v \n", taskID, worker)
					go func() {
						registerChan <- worker
					}()
					break
				}
				// task failed
				fmt.Printf("job %d NOT done by failed worker %v \n", taskID, worker)
			}
		}(i)
	}

	// Wait for all tasks to be done
	wg.Wait()
}
