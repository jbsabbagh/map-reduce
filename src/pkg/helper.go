package mr

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
)

func ReadFile(filename string) string {
	file, err := os.Open(filename)
	defer file.Close()

	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	return string(content)
}

func WriteRecord(ofile *os.File, key string, value string) {
	fmt.Fprintf(ofile, "%v %v\n", key, value)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type IntermediateFile struct {
	File  *os.File
	Mutex *sync.Mutex
}

func (f IntermediateFile) Write(key, value string) {
	f.Mutex.Lock()
	defer f.Mutex.Unlock()
	WriteRecord(f.File, key, value)
}

func (f IntermediateFile) Close() {
	f.File.Close()
}

func CreateOutputFiles(nReduce int, outputDir string) map[int]IntermediateFile {
	files := make(map[int]IntermediateFile)
	for index := 0; index < nReduce; index++ {
		oname := fmt.Sprintf("intermediate-%d", index)
		filepath := fmt.Sprintf("%s/%s", outputDir, oname)
		file, _ := os.Create(filepath)
		files[index] = IntermediateFile{File: file, Mutex: &sync.Mutex{}}
	}
	return files
}

func LoadDataFromIntermediateFile(file string) []KeyValue {
	fileHandle, _ := os.Open(file)
	defer fileHandle.Close()

	intermediate := []KeyValue{}
	scanner := bufio.NewScanner(fileHandle)
	for scanner.Scan() {
		line := scanner.Text()
		values := strings.Split(line, " ")
		intermediate = append(intermediate, KeyValue{Key: values[0], Value: values[1]})
	}
	sort.Sort(ByKey(intermediate))
	return intermediate
}
