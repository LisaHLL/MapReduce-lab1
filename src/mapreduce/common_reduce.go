package mapreduce

import (
	"encoding/json"
	//"fmt"
	"log"
	"os"
	"sort"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
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
	data := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		file, err := os.Open(reduceName(jobName, i, reduceTaskNumber))
		if err != nil {
			log.Fatal("Check commom_reduce.go", err)
		}
		defer file.Close()

		dec := json.NewDecoder(file)
		var kv KeyValue
		err = dec.Decode(&kv)
		for err == nil {
			_, exists := data[kv.Key]
			if !exists {
				data[kv.Key] = make([]string, 0)
			}
			data[kv.Key] = append(data[kv.Key], kv.Value)
			err = dec.Decode(&kv)
		}
		var keys []string
		for key, _ := range data {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		var res []KeyValue
		for _, key := range keys {
			v := reduceF(key, data[key])
			kv := KeyValue{Key: key, Value: v}
			res = append(res, kv)
		}

		output, err := os.Create(outFile)
		if err != nil {
			log.Fatal("Check commom_reduce.go", err)
		}
		defer output.Close()

		enc := json.NewEncoder(output)

		for _, kv := range res {
			enc.Encode(kv)
		}

	}
}

//type strs []string
//
//func (a strs) Len() int           { return len(a) }
//func (a strs) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
//func (a strs) Less(i, j int) bool { return a[i] < a[j] }
