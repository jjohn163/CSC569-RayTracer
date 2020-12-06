package main

import "fmt"
import "time"

type WorkAssignment struct {
    work         WorkItem
    workerId     int
    jobIndex     int
}


type AckMessage struct {
    workerId     int
    jobIndex     int
    isTakingWork bool
}

type KeyValue struct {
    Key   int
    Value string
}

const (
    maxWorkItems int = 100
    maxChanBuff  int = 10000
    INITIAL      int = 0
    MAPPING      int = 1
    REDUCE_INIT  int = 2
    REDUCING     int = 3
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var workTable []WorkAssignment
var numWorkers int
var numNeighbors int


func mapReduce(workers int, neighbors int, timeout int, scene Scene) {
    // Master ID is 0
    numWorkers = workers
    numNeighbors = neighbors

    initialTable := initHeartbeat(timeout)
    
    mapChannel := make(chan WorkAssignment, maxChanBuff)

    // pass in intermediate file name
    reduceChannel := make(chan WorkAssignment, maxChanBuff)
    ackChannel := make(chan AckMessage, maxChanBuff)

    for i := 1; i <= numWorkers; i++ {
        go worker(i, initialTable, mapChannel, reduceChannel, ackChannel, scene)
    }

    master(initialTable, mapChannel, reduceChannel, ackChannel, INITIAL, scene)
}


func worker(
    id int, 
    myTable HeartbeatTable, 
    mapChan chan WorkAssignment, 
    reduceChan chan WorkAssignment, 
    ackChannel chan AckMessage,
    scene Scene) {
   
    fmt.Printf("Worker: %d\n", id)
    continueMapping := true

    for continueMapping {
        select {
            case assignment, data := <- mapChan:
                if data {
                    doHeartbeat(id, &myTable)
                    ackChannel <- AckMessage{workerId: id, jobIndex: assignment.jobIndex, isTakingWork: true}
            
                    //map and write to IF
                    doHeartbeat(id, &myTable)
                    
                    kv := Map(assignment.work, &scene)
                    pixelRows[assignment.work] = kv.Value
            
                    //done with task
                    doHeartbeat(id, &myTable)
                    ackChannel <- AckMessage{workerId: id, jobIndex: assignment.jobIndex, isTakingWork: false}
                
                } else {
                    continueMapping = false
                }

            default:
                doHeartbeat(id, &myTable)

                time.Sleep(1e8)
        }
    }

    continueReducing := true
    
    for continueReducing {
        select {
            case assignment, data := <- reduceChan:
                if data {
                    doHeartbeat(id, &myTable)
                    ackChannel <- AckMessage{workerId: id, jobIndex: assignment.jobIndex, isTakingWork: true}

                    //reducing and writing to file
                    doHeartbeat(id, &myTable)

                    Reduce(&scene)

                    //done with task
                    doHeartbeat(id, &myTable)
                    ackChannel <- AckMessage{workerId: id, jobIndex: assignment.jobIndex, isTakingWork: false}
                } else {
                    continueReducing = false
                }
            default:
                doHeartbeat(id, &myTable)

                time.Sleep(5e8)
        }
    }
}


func master(
    myTable HeartbeatTable,
    mapChan chan WorkAssignment, 
    reduceChan chan WorkAssignment, 
    ackChannel chan AckMessage,
    phase int,
    scene Scene) {
    
    workTable = make([]WorkAssignment, scene.resY)
    
    fmt.Printf("Master\n")

    // Mapping
    jobId := 0
    // fill work table

    if phase == INITIAL {
        for i := 0; i < scene.resY; i++ {
            doHeartbeat(0, &myTable)
            var workItem WorkItem = WorkItem(i)

            workAssignment := WorkAssignment{workItem, -1, jobId}

            mapChan <- workAssignment
            workTable[jobId] = workAssignment
            jobId++
        }

        phase = MAPPING
    }

    workTableHasWork := phase == MAPPING
    for workTableHasWork {
        workTableHasWork = false

        ackChanIsEmpty := false
        for !ackChanIsEmpty {
            select {
                case acknowledgement := <- ackChannel:
                    doHeartbeat(0, &myTable)
                    if acknowledgement.isTakingWork {
                        // Update work table with worker id
                        for i := 0; i < len(workTable); i++ {
                            if workTable[i].jobIndex == acknowledgement.jobIndex {
                                workTable[i].workerId = acknowledgement.workerId
                                break
                            }
                        }
                    } else {
                        // Remove work assignment from work table
                        for i := 0; i < len(workTable); i++ {
                            if workTable[i].jobIndex == acknowledgement.jobIndex {
                                workTable[i] = WorkAssignment{}
                            }
                        }
                    }
                default:
                    ackChanIsEmpty = true
            }
        }

        // Check for failed workers
        failedWorkAssignment := checkTable(0, &myTable)
        if failedWorkAssignment != -1 {
            for i := 0; i < len(workTable); i++ {
                if workTable[i].workerId == failedWorkAssignment {
                    fmt.Printf("Goroutine stopped working, relaunching..., %s\n", failedWorkAssignment, time.Now().String())
                    go worker(failedWorkAssignment, myTable, mapChan, reduceChan, ackChannel, scene)

                    workTable[i].workerId = -1
                    workAssignment := workTable[i]
                    mapChan <- workAssignment
                }
            }
            doHeartbeat(0, &myTable)
        }

        // Check if work table is empty
        emptyWorkAssignment := WorkAssignment{}
        for i := 0; i < len(workTable); i++ {
            if workTable[i] != emptyWorkAssignment {
                workTableHasWork = true
            }
        }
        
        doHeartbeat(0, &myTable)
        time.Sleep(1e9)
    }

    if phase == MAPPING {
        close(mapChan)
        phase = REDUCE_INIT
    }

    // Reducing
    if phase == REDUCE_INIT {
        jobId = 0

        doHeartbeat(0, &myTable)
        var workItem WorkItem = WorkItem(1)
        workAssignment := WorkAssignment{workItem, -1, jobId}

        reduceChan <- workAssignment
        workTable[jobId] = workAssignment
        jobId++

        phase = REDUCING
    }

    workTableHasWork = true
    for workTableHasWork {
        workTableHasWork = false

        ackChanIsEmpty := false
        for !ackChanIsEmpty {
            select {
                case acknowledgement := <- ackChannel:
                    doHeartbeat(0, &myTable)
                    if acknowledgement.isTakingWork {
                        // Update work table with worker id
                        for i := 0; i < len(workTable); i++ {
                            if workTable[i].jobIndex == acknowledgement.jobIndex {
                                workTable[i].workerId = acknowledgement.workerId
                                break
                            }
                        }
                    } else {
                        // Remove work assignment from work table
                        for i := 0; i < len(workTable); i++ {
                            if workTable[i].jobIndex == acknowledgement.jobIndex {
                                workTable[i] = WorkAssignment{}
                            }
                        }
                    }
                default:
                    ackChanIsEmpty = true
            }
        }

        // Check for failed workers
        failedWorkAssignment := checkTable(0, &myTable)
        if failedWorkAssignment != -1 {
            for i := 0; i < len(workTable); i++ {
                if workTable[i].workerId == failedWorkAssignment {
                    fmt.Printf("Goroutine %d stopped working, relaunching..., %s\n", failedWorkAssignment, time.Now().String())
                    go worker(failedWorkAssignment, myTable, mapChan, reduceChan, ackChannel, scene)

                    workTable[i].workerId = -1
                    workAssignment := workTable[i]
                    reduceChan <- workAssignment
                }
            }
            doHeartbeat(0, &myTable)
        }

        // Check if work table is empty
        emptyWorkAssignment := WorkAssignment{}
        for i := 0; i < len(workTable); i++ {
            if workTable[i] != emptyWorkAssignment {
                workTableHasWork = true
            }
        }

        doHeartbeat(0, &myTable)
        time.Sleep(1e9)
    }

    close(reduceChan)
    fmt.Printf("Master is done\n")
}
