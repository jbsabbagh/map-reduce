package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import (
	"fmt"
	mr "map-reduce/src/pkg"
	"os"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}

	coordinator := mr.MakeCoordinator(os.Args[1:], 10)
	for coordinator.Done() == false {
		time.Sleep(time.Second)
	}
	time.Sleep(time.Second)
}
