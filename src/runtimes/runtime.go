package runtimes

import (
	mr "map-reduce/src/pkg"
)

type Runtime interface {
	Run(app *mr.MapReduceApp)
}
