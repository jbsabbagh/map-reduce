package runtimes

import (
	"fmt"
	"io"
	"log"
	"os"
)

func readFile(filename string) string {
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

func writeRecord(ofile *os.File, key string, value string) {
	fmt.Fprintf(ofile, "%v %v\n", key, value)
}
