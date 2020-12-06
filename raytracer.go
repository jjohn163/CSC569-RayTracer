package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"
)

type WorkItem int

var pixelRows []string

var workers = flag.String("w", "1", "number of worker nodes")
var neighbors = flag.String("n", "2", "number of nodes neighbors")
var samples = flag.String("s", "50", "number of samples per pixel")
var depth = flag.String("d", "50", "depth of each ray")


func main() {
	flag.Parse()
	
	numWorkers, _ := strconv.Atoi(*workers)
	numNeighbors, _ := strconv.Atoi(*neighbors)

	scene := scene1()
	pixelRows = make([]string, scene.resY)
	mapReduce(numWorkers, numNeighbors, 50, scene)
}


func Map(rowNum WorkItem, scene *Scene) KeyValue {
	// Given row num, raytrace pixels
	row := ""
	r := rand.New(rand.NewSource(99))
	numSamples, _ := strconv.Atoi(*samples)
	rayDepth, _ := strconv.Atoi(*depth)

	for x := 0; x < scene.resX; x++ {
		color := scene.trace(x, int(rowNum), numSamples, rayDepth, r)
		row += color.toStringColor()
	}

	row += "\n"
	kv := KeyValue{int(rowNum), row}
	
	return kv
}


func Reduce(scene *Scene) {
	// One reducer outputs output.ppm file
	f, err := os.Create("output.ppm")
	if err != nil {
		log.Fatal(err)
	}
	
	//write header for output file
	f.WriteString("P3\n")
	f.WriteString(fmt.Sprintf("%d %d\n", scene.resX, scene.resY))
	f.WriteString("255\n")
	
	//trace the scene
	for y := scene.resY - 1; y >= 0; y--{
		if(pixelRows[y] != "") {
				f.WriteString(pixelRows[y])
		} else {
				y++
				time.Sleep(1e6)
		}
	}
	
	f.Close()
}
