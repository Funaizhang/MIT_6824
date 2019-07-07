package mapreduce

import (
	"encoding/json"
	"log"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	// create a map of kv pairs
	kvs := make(map[string][]string)

	for m := 0; m < nMap; m++ {

		// read intermediate file, there are nMap of them for each reduce task
		interFilename := reduceName(jobName, m, reduceTask)
		interFile, err_open := os.Open(interFilename)
		if err_open != nil {
			log.Fatal(err_open)
		}

		dec := json.NewDecoder(interFile)
		// while the array contains values
		for dec.More() {
			var kv KeyValue
			// decode an array value (interFile)
			err_decode := dec.Decode(&kv)
			if err_decode != nil {
				log.Fatal(err_decode)
			}

			// store kv in kvs map
			kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
		}

		// close interFile
		if err_close := interFile.Close(); err_close != nil {
			log.Fatal(err_close)
		}
	}

	// make string slice containing keys from kvs
	keysSorted := make([]string, 0)
	for key := range kvs {
		keysSorted = append(keysSorted, key)
	}
	// sort keys in keysSorted slice
	sort.Strings(keysSorted)

	// Create output file
	file, err_create := os.Create(outFile)
	if err_create != nil {
		log.Fatal(err_create)
	}

	enc := json.NewEncoder(file)
	// Call reduceF on each key and write on output file
	for _, key := range keysSorted {
		result := reduceF(key, kvs[key])
		err_encode := enc.Encode(KeyValue{key, result})
		if err_encode != nil {
			log.Fatal(err_encode)
		}
	}

	if err_close := file.Close(); err_close != nil {
		log.Fatal(err_close)
	}
}
