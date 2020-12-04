package main

import "fmt"
import "log"
import "os"

type WorkItem string


func main() {
	mapReduce(os.Args[1:], 8, 2, 10, false)
}


func Map(filename string, contents string) []KeyValue{
	// Given ??? raytrace pixel (n times?)
}


func Reduce(key string, values []string) string{
	// One reducer outputs output.ppm file
}


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
}