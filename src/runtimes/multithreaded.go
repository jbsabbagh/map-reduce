package runtimes

import (
	"fmt"
	"hash/fnv"
	"log"
	mr "map-reduce/src/pkg"
	"os"
	"sort"
	"sync"
)

type MultithreadedRuntime struct{}

func (r MultithreadedRuntime) Run(app *mr.MapReduceApp) {
	r.callMap(app.MapFunction, app.InputFilenames)
	r.callReduce(app.ReduceFunction)
}

func (r MultithreadedRuntime) callMap(mapf func(string, string) []mr.KeyValue, filenames []string) {
	files := mr.CreateOutputFiles(BUCKETS, INTERMEDIATE_DIR)

	var wg sync.WaitGroup
	for _, filename := range filenames {
		wg.Add(1)
		go func(filename string, files map[int]mr.IntermediateFile) {
			defer wg.Done()
			content := mr.ReadFile(filename)
			keyValues := mapf(filename, content)

			for _, kv := range keyValues {
				bucket := ihash(kv.Key) % BUCKETS
				file := files[bucket]
				file.Write(kv.Key, kv.Value)
			}
		}(filename, files)
	}
	wg.Wait()

	for _, file := range files {
		file.Close()
	}
}

func (r MultithreadedRuntime) callReduce(reducef func(string, []string) string) {
	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	files, err := os.ReadDir(INTERMEDIATE_DIR)
	if err != nil {
		log.Fatal(err)
	}

	var wg sync.WaitGroup
	for index, file := range files {
		outputFile, _ := os.Create(fmt.Sprintf("%s/mr-out-%d", OUTPUT_DIR, index))
		wg.Add(1)
		go func(file os.DirEntry, outputFile *os.File) {
			defer wg.Done()

			intermediate := mr.LoadDataFromIntermediateFile(INTERMEDIATE_DIR + file.Name())
			sort.Sort(mr.ByKey(intermediate))
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				mr.WriteRecord(outputFile, intermediate[i].Key, output)

				i = j
			}
			outputFile.Close()
		}(file, outputFile)
	}
	wg.Wait()
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
