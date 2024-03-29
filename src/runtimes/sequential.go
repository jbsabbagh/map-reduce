package runtimes

import (
	mr "map-reduce/src/pkg"
	"os"
	"sort"
)

// for sorting by key.

type SequentialRuntime struct{}

func (r SequentialRuntime) Run(app *mr.MapReduceApp) {
	intermediate := r.callMap(app.MapFunction, app.InputFilenames)
	sort.Sort(mr.ByKey(intermediate))
	r.callReduce(app.ReduceFunction, intermediate)
}

func (r SequentialRuntime) callMap(mapf func(string, string) []mr.KeyValue, filenames []string) []mr.KeyValue {
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	//
	intermediate := []mr.KeyValue{}
	for _, filename := range filenames {
		content := mr.ReadFile(filename)
		kva := mapf(filename, content)

		//
		// a big difference from real MapReduce is that all the
		// intermediate data is in one place, intermediate[],
		// rather than being partitioned into NxM buckets.
		//
		intermediate = append(intermediate, kva...)

	}
	return intermediate
}

func (r SequentialRuntime) callReduce(reducef func(string, []string) string, intermediate []mr.KeyValue) {
	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//

	oname := "../data/out/mr-out-0"
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

		mr.WriteRecord(ofile, intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}
