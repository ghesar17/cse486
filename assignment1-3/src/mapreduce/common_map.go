package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"math"
	"os"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	data, err := os.ReadFile(inFile)
	checkError(err)

	// get the processed key value pairs
	pairs := mapF(inFile,string(data))

	// use map to store fileName to encoder
	encoders := make(map[int]*json.Encoder)

	for _, kv := range pairs {
		// determine which encoder a pair should go to 
		reduceNumber := int(math.Mod(float64(ihash(kv.Key)),
		float64(nReduce)))
		_, in := encoders[reduceNumber]
		// if encoder already exists, if it does encode the keyvalue
		if in {
			encoders[reduceNumber].Encode(&kv)
		}
		// if the encoder doesnt exist, create it and encode the keyvalue
		if !in {
			// create the file and encoder
			fileName := reduceName(jobName,mapTaskNumber,reduceNumber)
			file, err := os.Create(fileName)
			checkError(err)
			defer file.Close()
			encoders[reduceNumber] = json.NewEncoder(file)
			encoders[reduceNumber].Encode(&kv)
		}
	}
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
