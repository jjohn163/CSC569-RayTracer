package main

import "fmt"
import "log"
import "os"
import "time"

type WorkItem int

var g_scene Scene = scene1()

var pixelRows []string = make([]string, g_scene.resY)

const (
    SAMPLES int = 50
    DEPTH   int = 50
)


func main() {
	mapReduce(8, 2, 50, false)
}


func Map(rowNum WorkItem) KeyValue {
	// Given row num, raytrace pixels
	row := ""

	for x := 0; x < g_scene.resX; x++ {
		color := g_scene.trace(x, int(rowNum), SAMPLES, DEPTH)
		row += color.toStringColor()
	}

	row += "\n"
	kv := KeyValue{int(rowNum), row}
	
	return kv
}


func Reduce() {
	// One reducer outputs output.ppm file
	f, err := os.Create("output.ppm")
	if err != nil {
		log.Fatal(err)
	}
	
	//write header for output file
	f.WriteString("P3\n")
	f.WriteString(fmt.Sprintf("%d %d\n", g_scene.resX, g_scene.resY))
	f.WriteString("255\n")
	
	//trace the scene
	for y := g_scene.resY - 1; y >= 0; y--{
		if(pixelRows[y] != "") {
				f.WriteString(pixelRows[y])
		} else {
				y++
				time.Sleep(1e6)
		}
	}
	
	f.Close()
}
