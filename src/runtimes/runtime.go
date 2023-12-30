package runtimes

import (
	mr "map-reduce/src/pkg"
)

type Runtime interface {
	Run(app *mr.MapReduceApp)
	// callReduce(reduceFunction func(string, []string) string, intermediate []mr.KeyValue)
	// callMap(mapFunction func(string, string) []mr.KeyValue, filenames []string) []mr.KeyValue
}
