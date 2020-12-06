package main

import "flag"
import "fmt"
import "log"
import "math/rand"
import "os"
import "time"
import "runtime"
import "runtime/pprof"

type WorkItem int

var pixelRows []string
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")

const (
    SAMPLES int = 50
    DEPTH   int = 50
)


func main() {
	flag.Parse()
    if *cpuprofile != "" {
        f, err := os.Create(*cpuprofile)
        if err != nil {
            log.Fatal("could not create CPU profile: ", err)
        }
        defer f.Close() // error handling omitted for example
        if err := pprof.StartCPUProfile(f); err != nil {
            log.Fatal("could not start CPU profile: ", err)
        }
        defer pprof.StopCPUProfile()
    }


	scene := scene1()
	pixelRows = make([]string, scene.resY)
	mapReduce(8, 2, 50, false, scene)

    if *memprofile != "" {
        f, err := os.Create(*memprofile)
        if err != nil {
            log.Fatal("could not create memory profile: ", err)
        }
        defer f.Close() // error handling omitted for example
        runtime.GC() // get up-to-date statistics
        if err := pprof.WriteHeapProfile(f); err != nil {
            log.Fatal("could not write memory profile: ", err)
        }
    }
}


func Map(rowNum WorkItem, scene *Scene) KeyValue {
	// Given row num, raytrace pixels
	row := ""
	r := rand.New(rand.NewSource(99))

	for x := 0; x < scene.resX; x++ {
		color := scene.trace(x, int(rowNum), SAMPLES, DEPTH, r)
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
