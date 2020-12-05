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
		if(pixelRows[y] == "") {
				f.WriteString(pixelRows[y])
		} else {
				time.Sleep(1e6)
		}
	}
	
	f.Close()
}

/*
func main() {
	f, err := os.Create("output.ppm")
    if err != nil {
        log.Fatal(err)
    }
    defer f.Close()

	scene := scene1()
	
	//write header for output file
	f.WriteString("P3\n")
	f.WriteString(fmt.Sprintf("%d %d\n", scene.resX, scene.resY))
	f.WriteString("255\n")
	
	//trace the scene
	for y := scene.resY - 1; y >= 0; y--{
		for x := 0; x < scene.resX; x++ {
			color := scene.trace(x, y, 50, 50)
			f.WriteString(color.toStringColor())
		}
		f.WriteString("\n") //newline for next row
	}

    fmt.Println("done")
}*/