package main

import (
	"fmt"
	"log"
	"mapreduce"
	"os"
	"strconv"
	"strings"
	"unicode"
)

// The mapping function is called once for each piece of the input.
// In this framework, the key is the name of the file that is being processed,
// and the value is the file's contents. The return value should be a slice of
// key/value pairs, each represented by a mapreduce.KeyValue.
func mapF(document string, value string) (res []mapreduce.KeyValue) {
	splitByNonLetter := func(c rune) bool {
		return !unicode.IsLetter(c)
	}
	// splits contents by non letters
	wordArray := strings.FieldsFunc(value, splitByNonLetter)

	wordMap := make(map[string]mapreduce.KeyValue)
	for _, word := range wordArray {
		_, in := wordMap[word]
		// if first occurence of word, create new keyvalue
		if !in {
			wordMap[word] = mapreduce.KeyValue{Key: word, Value: "1"}
		} else {
			// if word has already exists, increment its value
			current_count, err := strconv.Atoi(wordMap[word].Value)
			if err != nil {
				log.Fatal(err)
			}
			newCount := strconv.Itoa(current_count + 1)
			wordMap[word] = mapreduce.KeyValue{Key: word, Value: newCount}
		}
	}
	//put all KeyValues from hashmap into array
	result := make([]mapreduce.KeyValue, len(wordMap))
	for _, pair := range wordMap {
		result = append(result, pair)
	}
	return result
}


// The reduce function is called once for each key generated by Map, with a
// list of that key's string value (merged across all inputs). The return value
// should be a single output value for that key.
func reduceF(key string, values []string) string {
	sum := 0
	// sum the counts of a word
	for _, count := range values {
		num, err := strconv.Atoi(count)
		if err == nil {
			sum += num
		} else {
			break
		}
	}
	result := strconv.Itoa(sum)
	return result
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master sequential x1.txt .. xN.txt)
// 2) Master (e.g., go run wc.go master localhost:7777 x1.txt .. xN.txt)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
	if len(os.Args) < 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		var mr *mapreduce.Master
		if os.Args[2] == "sequential" {
			mr = mapreduce.Sequential("wcseq", os.Args[3:], 3, mapF, reduceF)
		} else {
			mr = mapreduce.Distributed("wcseq", os.Args[3:], 3, os.Args[2])
		}
		mr.Wait()
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], mapF, reduceF, 100)
	}
}
