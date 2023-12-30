package main

//
// simple sequential MapReduce.
//
// go run mrsequential.go wc.so pg*.txt
//

import (
	"log"
	mr "map-reduce/src/pkg"
	runtime "map-reduce/src/runtimes"
	"os"
	"plugin"
)

func main() {
	args := ParsedArgs{}
	args.parse()

	mapf, reducef := loadPlugin(args.PluginFilename)

	app := mr.MapReduceApp{
		InputFilenames: args.InputFilenames,
		MapFunction:    mapf,
		ReduceFunction: reducef,
	}

	runtime := runtime.MultithreadedRuntime{}

	runtime.Run(&app)
}

type ParsedArgs struct {
	PluginFilename string
	InputFilenames []string
}

func (p *ParsedArgs) parse() {
	if len(os.Args) < 3 {
		log.Fatalf("Usage: mrsequential xxx.so inputfiles...\n")
	}

	p.PluginFilename = os.Args[1]
	p.InputFilenames = os.Args[2:]
}

// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
