package runtimes

import (
	mr "map-reduce/src/pkg"
)

type DistributedRuntime struct{}

func (r DistributedRuntime) Run(app *mr.MapReduceApp) {
	coordinator := mr.MakeCoordinator(app.InputFilenames, BUCKETS)
	coordinator.Run()
}

func (r DistributedRuntime) RunCoordinator(coordinator *mr.Coordinator) {
}
