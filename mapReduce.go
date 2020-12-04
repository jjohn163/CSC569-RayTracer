package main

import (
    "fmt"
    "time"
    "encoding/json"
    "os"
    "log"
    "strings"
    "io/ioutil"
    "sort"
    "sync"
)


type WorkAssignment struct {
    work         WorkItem
    intermediate string
    workerId     int
    jobIndex     int
}


type AckMessage struct {
    workerId     int
    jobIndex     int
    isTakingWork bool
}

type KeyValue struct {
    Key   string
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
var mu sync.Mutex


func mapReduce(fileNames []string, workers int, neighbors int, timeout int, crash bool) {
    // Master ID is 0
    numWorkers = workers
    numNeighbors = neighbors

    initialTable := initHeartbeat(timeout)
    
    mapChannel := make(chan WorkAssignment, maxChanBuff)

    // pass in intermediate file name
    reduceChannel := make(chan WorkAssignment, maxChanBuff)
    ackChannel := make(chan AckMessage, maxChanBuff)

    for i := 1; i <= numWorkers; i++ {
        go worker(i, initialTable, mapChannel, reduceChannel, ackChannel, crash, fileNames)
    }

    //master(initialTable, mapChannel, reduceChannel, ackChannel, fileNames, INITIAL)

    time.Sleep(60e9)
}


func worker(
    id int, 
    myTable HeartbeatTable, 
    mapChan chan WorkAssignment, 
    reduceChan chan WorkAssignment, 
    ackChannel chan AckMessage,
    crash bool,
    filenames []string) {
   
    fmt.Printf("Worker: %d\n", id)
    continueMapping := true
    
    if (crash && id == 1) {
        fmt.Printf("Forcing worker %d to crash\n", id)
        return
    }
    
    phase := INITIAL

    for continueMapping {
        select {
            case assignment, data := <- mapChan:
                phase = MAPPING
                if data {
                    doHeartbeat(id, &myTable)
                    ackChannel <- AckMessage{workerId: id, jobIndex: assignment.jobIndex, isTakingWork: true}
            
                    //open file contents for mapping
                    doHeartbeat(id, &myTable)
                    
                    file, err := os.Open(string(assignment.work))
                    
                    if err != nil {
                        log.Fatalf("cannot open %v", assignment.work)
                    }
                    
                    content, err := ioutil.ReadAll(file)
                    
                    if err != nil {
                        log.Fatalf("cannot read %v", assignment.work)
                    }
            
                    //map and write to IF
                    doHeartbeat(id, &myTable)
                    
                    kva := Map(assignment.intermediate, string(content))

                    doHeartbeat(id, &myTable)

                    ifile, err := os.Create(assignment.intermediate)
                    
                    if err != nil {
                        log.Fatalf("cannot open %v", assignment.intermediate)
                    }
                    
                    enc := json.NewEncoder(ifile)
                    
                    for _, kv := range kva {
                        if err := enc.Encode(&kv); err != nil {
                            break
                        }
                    }
                    
                    ifile.Close()
            
                    //done with task
                    doHeartbeat(id, &myTable)
                    ackChannel <- AckMessage{workerId: id, jobIndex: assignment.jobIndex, isTakingWork: false}
                
                } else {
                    continueMapping = false
                    phase = REDUCE_INIT
                }

            default:
                doHeartbeat(id, &myTable)

                mu.Lock()
                if checkTable(id, &myTable) == 0 {
                    fmt.Printf("Master stopped working, relaunching..., %s\n", time.Now().String())
                    go master(myTable, mapChan, reduceChan, ackChannel, filenames, phase)
				}
                mu.Unlock()

                time.Sleep(1e8)
        }
    }

    mu.Lock()
    if checkTable(id, &myTable) == 0 {
        fmt.Printf("Master stopped working, relaunching..., %s\n", time.Now().String())
        go master(myTable, mapChan, reduceChan, ackChannel, filenames, phase)  
	}
    mu.Unlock()

    continueReducing := true
    
    for continueReducing {
        select {
            case assignment, data := <- reduceChan:
                phase = REDUCING
                if data {
                    doHeartbeat(id, &myTable)
                    ackChannel <- AckMessage{workerId: id, jobIndex: assignment.jobIndex, isTakingWork: true}
                    kva := []KeyValue{}

                    //reading IF for reduce
                    doHeartbeat(id, &myTable)
                    
                    ifile, err := os.Open(assignment.intermediate)
                    
                    if err != nil {
                        log.Fatalf("cannot open %v", assignment.intermediate)
                    }
                    
                    dec := json.NewDecoder(ifile)
                    
                    for {
                        var kv KeyValue
                        if err := dec.Decode(&kv); err != nil {
                            break
                        } 
                        kva = append(kva, kv)
                    }
                    
                    ifile.Close()
                    sort.Sort(ByKey(kva))

                    //reducing and writing to file
                    doHeartbeat(id, &myTable)
                    
                    fileArray := strings.Split(assignment.intermediate, "-")
                    oname := "mr-out-" + fileArray[len(fileArray)-1]
                    ofile, _ := os.Create(oname)
                    
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
                        output := Reduce(kva[i].Key, values)
                        // this is the correct format for each line of Reduce output.
                        fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

                        i = j
                    }

                    //done with task
                    doHeartbeat(id, &myTable)
                    ackChannel <- AckMessage{workerId: id, jobIndex: assignment.jobIndex, isTakingWork: false}

                    //cleanup IF
                    e := os.Remove(assignment.intermediate) 
                    if e != nil { 
                        log.Fatal(e) 
                    } 
                } else {
                    continueReducing = false
                }
            default:
                doHeartbeat(id, &myTable)

                mu.Lock()
                if checkTable(id, &myTable) == 0 {
                    fmt.Printf("Master stopped working, relaunching..., %s\n", time.Now().String())
                    go master(myTable, mapChan, reduceChan, ackChannel, filenames, phase)
				}
                mu.Unlock()

                time.Sleep(5e8)
        }
    }
}


func master(
    myTable HeartbeatTable,
    mapChan chan WorkAssignment, 
    reduceChan chan WorkAssignment, 
    ackChannel chan AckMessage,
    fileNames []string,
    phase int) {
    
    workTable = make([]WorkAssignment, len(fileNames))
    var intermediateFiles []string
    
    fmt.Printf("Master\n")

    // Mapping
    jobId := 0
    // fill work table

    if phase == INITIAL {
        for _, filename := range fileNames {
            doHeartbeat(0, &myTable)
            var workItem WorkItem = WorkItem(filename)
            fileArray := strings.Split(filename, "/")
            intermediateFile := "if-" + fileArray[len(fileArray) -1]
            intermediateFiles = append(intermediateFiles, intermediateFile)

            workAssignment := WorkAssignment{workItem, intermediateFile, -1, jobId}

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
                    go worker(failedWorkAssignment, myTable, mapChan, reduceChan, ackChannel, false, fileNames)

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
        // Force master to crash at this stage
        return
    }

    // Reducing
    if phase == REDUCE_INIT {
        jobId = 0
        for _, filename := range intermediateFiles {
            doHeartbeat(0, &myTable)
            var workItem WorkItem = WorkItem(filename)
            workAssignment := WorkAssignment{workItem, filename, -1, jobId}

            reduceChan <- workAssignment
            workTable[jobId] = workAssignment
            jobId++
        }

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
                    go worker(failedWorkAssignment, myTable, mapChan, reduceChan, ackChannel, false, fileNames)

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
}
