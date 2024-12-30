package mapreduce

import (
	"encoding/json"
	"os"
)

type WordCount struct {
	Key string 
	Value string 
}

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	wordMap := make(map[string][]string)

	for i := 0; i < nMap; i++ {
		// determine files to read from
		fileName := reduceName(jobName,i,reduceTaskNumber)
		file, err := os.Open(fileName)
		checkError(err)
		defer file.Close()
		var decoder *json.Decoder = json.NewDecoder(file)
		// decode the data
		for {
			// create temp variable to store data
			word := WordCount{}
			err = decoder.Decode(&word)
			// continue decoding until error is returned
			if err == nil {
				_, in := wordMap[word.Key]
				// group each word's counts together into an array
				if !in {
					var counts []string
					counts = append(counts, word.Value)
					wordMap[word.Key] = counts
				} else {
					wordMap[word.Key] = append(wordMap[word.Key],word.Value)
			}
		} else {
				break
			}
    	}
	}

	mergeFileName :=  mergeName(jobName,reduceTaskNumber)
	file, err := os.Create(mergeFileName)
	checkError(err)
	enc := json.NewEncoder(file)
	// encode for final output file
	for word, counts := range wordMap {
		enc.Encode(KeyValue{word, reduceF(word,counts)})
	}
	file.Close()
}