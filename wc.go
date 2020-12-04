package main;

import "os"
import "unicode"
import "strings"
import "strconv"

type WorkItem string


func main() {
	if (os.Args[1] == "-c") {
		mapReduce(os.Args[2:], 8, 2, 10, true)	
	} else {
		mapReduce(os.Args[1:], 8, 2, 10, false)
	}
}


func Map(filename string, contents string) []KeyValue{
	// function to detect word separators.
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	// split contents into an array of words.
	words := strings.FieldsFunc(contents, ff)

	kva := []KeyValue{}
	for _, w := range words {
		kv := KeyValue{w, "1"}
		kva = append(kva, kv)
	}
	return kva
}

func Reduce(key string, values []string) string{
	// return the number of occurrences of this word.
	return strconv.Itoa(len(values))
}