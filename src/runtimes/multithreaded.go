package runtimes

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"log"
	mr "map-reduce/src/pkg"
	"os"
	"sort"
	"strings"
	"sync"
)

const BUCKETS = 10

// for sorting by key.
type ByKey []mr.KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type SyncedFile struct {
	File  *os.File
	Mutex *sync.Mutex
}

func (f SyncedFile) Write(kv mr.KeyValue) {
	f.Mutex.Lock()
	defer f.Mutex.Unlock()
	writeRecord(f.File, kv.Key, kv.Value)
}

func (f SyncedFile) Close() {
	f.File.Close()
}

type MultithreadedRuntime struct{}

func (r MultithreadedRuntime) Run(app *mr.MapReduceApp) {
	r.callMap(app.MapFunction, app.InputFilenames)
	r.callReduce(app.ReduceFunction)
}

func (r MultithreadedRuntime) callMap(mapf func(string, string) []mr.KeyValue, filenames []string) {
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	//
	// intermediate := []mr.KeyValue{}
	files := make(map[int]SyncedFile)

	for index := 0; index < BUCKETS; index++ {
		oname := fmt.Sprintf("intermediate-%d", index)
		filepath := fmt.Sprintf("data/intermediate/%s", oname)
		file, _ := os.Create(filepath)
		files[index] = SyncedFile{File: file, Mutex: &sync.Mutex{}}
	}

	var wg sync.WaitGroup
	for _, filename := range filenames {
		wg.Add(1)
		go func(filename string, files map[int]SyncedFile) {
			defer wg.Done()
			content := readFile(filename)
			keyValues := mapf(filename, content)

			for _, kv := range keyValues {
				bucket := ihash(kv.Key) % BUCKETS
				file := files[bucket]
				file.Write(kv)
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
	dir := "data/intermediate/"
	files, err := os.ReadDir(dir)
	intermediate := []mr.KeyValue{}

	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		fileHandle, _ := os.Open(dir + file.Name())
		defer fileHandle.Close()
		scanner := bufio.NewScanner(fileHandle)
		for scanner.Scan() {
			line := scanner.Text()
			values := strings.Split(line, " ")
			intermediate = append(intermediate, mr.KeyValue{Key: values[0], Value: values[1]})
		}
	}
	sort.Sort(ByKey(intermediate))
	oname := "mr-out-0"
	ofile, _ := os.Create(oname)

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

		writeRecord(ofile, intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
