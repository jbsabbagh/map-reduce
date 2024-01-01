package mr

type MapReduceApp struct {
	InputFilenames []string
	MapFunction    func(string, string) []KeyValue
	ReduceFunction func(string, []string) string
}

type KeyValue struct {
	Key   string
	Value string
}
