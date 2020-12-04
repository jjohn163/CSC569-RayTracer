package main


import (
    "fmt"
    "math/rand"
    "time"
)


type Heartbeat struct {
    id        int
    hbcounter int
    time      time.Time
}

type HeartbeatTable struct {
    heartbeats []Heartbeat  
} 

var channels []chan HeartbeatTable
var printMutex = make(chan int, 1)
var maxTimeout time.Duration


func initHeartbeat(timeout int) HeartbeatTable {
    myTable := HeartbeatTable{}
    maxTimeout = time.Duration(timeout * 1e9)
    
    // len of heartbeat table = numWorkers + 1 master
    myTable.heartbeats = make([]Heartbeat, numWorkers + 1)

    channels = make([]chan HeartbeatTable, numWorkers + 1)

    for i := 0; i <= numWorkers; i++ {
        // init channel
        channels[i] = make(chan HeartbeatTable, maxChanBuff)

        // init Heartbeats
        myTable.heartbeats[i] = Heartbeat{i, 0, time.Now()}
    }

    return myTable
}


func doHeartbeat(id int, table *HeartbeatTable) {
    sendHeartbeat(id, table)
    receiveHeartbeats(id, table)
}


func sendHeartbeat(id int, table *HeartbeatTable) {
    // Update our heartbeat
    table.heartbeats[id].hbcounter++
    table.heartbeats[id].time = time.Now()

    for i := 0; i < numNeighbors; i++ {
        // Send heartbeat to numNeighbors random channels, not including ours.
        neighbor := rand.Intn(numWorkers)

        if (neighbor == id) {
            neighbor = numWorkers
        }

        channels[neighbor] <- *table
    }
}


func receiveHeartbeats(id int, myTable *HeartbeatTable) {
    chanIsEmpty := false

    // Recieve tables until empty
    for !chanIsEmpty {
        select {
            case newTable := <- channels[id]:
                combineTables(myTable, &newTable)
            
            default:
                chanIsEmpty = true
        }
    }
}


func combineTables(oldTable *HeartbeatTable, newTable *HeartbeatTable) {
    for i := 0; i <= numWorkers; i++ {
        if (newTable.heartbeats[i].hbcounter > newTable.heartbeats[i].hbcounter) {
            oldTable.heartbeats[i] = newTable.heartbeats[i]
            oldTable.heartbeats[i].time = time.Now()
        }
    }
}


func checkTable(id int, table *HeartbeatTable) int {
    minIdx := -1
    maxIdx := -1

    receiveHeartbeats(id, table)

    for i := 0; i <= numWorkers; i++ {
        tempTime := table.heartbeats[i].time

        if (maxIdx == -1 ||
            tempTime.Sub(table.heartbeats[maxIdx].time) > 0) {
            maxIdx = i
        }

        if (minIdx == -1 ||
            table.heartbeats[minIdx].time.Sub(tempTime) > 0) {
            minIdx = i
        }
    }

    if (minIdx == -1 || maxIdx == -1) {
        return -1
    }

    if (table.heartbeats[maxIdx].time.Sub(table.heartbeats[minIdx].time) > maxTimeout) {
        table.heartbeats[minIdx].time = time.Now()
        return minIdx
    }

    return -1
}


func printTable(table *HeartbeatTable, id int) {
    printMutex <- 0

    fmt.Println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
    fmt.Printf("Worker %d table\n", id)

    for i := 0; i <= numWorkers; i++ {
        fmt.Printf("Neighbor %d:\n", i)
        fmt.Printf("\tId: %d\n", table.heartbeats[i].id)
        fmt.Printf("\tHb Counter: %d\n", table.heartbeats[i].hbcounter)
        fmt.Printf("\tTime: %s\n", table.heartbeats[i].time.String())
        fmt.Printf("---------------------------------\n")
    }
    fmt.Println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")

    <- printMutex
}

