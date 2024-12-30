package mapreduce

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)

	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}
	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)
	mr.doTasks(ntasks, nios, phase)
	debug("Schedule: %v phase done\n", phase)
}

func (mr *Master) doTasks(
	ntasks int,
	nios int,
	phase jobPhase,
) {
	finishedChan := make(chan int, ntasks)
	remainingChan := make(chan int, ntasks)

	for i := 0; i < ntasks; i++ {
		remainingChan <- i
	}

	for{
		select {
		case taskName := <-remainingChan:
			worker := <-mr.registerChannel
			go func() {
				taskArgs := DoTaskArgs{
					JobName:    mr.jobName,
					File:       mr.files[taskName],
					Phase:      phase,
					TaskNumber: taskName,
					NumOtherPhase: nios,
				}
				ok := call(worker, "Worker.DoTask", &taskArgs, new(struct{}))
				if ok {
					mr.registerChannel <- worker
					finishedChan <- taskName
				} else {
					remainingChan <- taskName
				}
				}()
		}
		if (len(remainingChan) == 0){
			break
		} 
	}	
}