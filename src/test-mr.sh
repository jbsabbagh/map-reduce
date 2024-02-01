# #!/bin/bash

set -e

TIMEOUT=$"gtimeout -k 2s 30s"

start=$(date +%s)
echo "Running test..."
go build -race -buildmode=plugin apps/wordcount/wc.go
go build -race runtimes/mrcoordinator.go
go build -race runtimes/mrworker.go
$TIMEOUT ./mrcoordinator ../data/pg-*.txt &
$TIMEOUT ./mrworker wc.so &
$TIMEOUT ./mrworker wc.so
# go run main.go wc.so ../data/pg*.txt
end=$(date +%s)

echo "Time elapsed: $((end-start))s"
# exit 0


cat ../data/out/out-* | sort > ../data/out-all

if diff -q ../data/correct ../data/out-all >/dev/null; then
    echo -e "\033[32mPass\033[0m"
else
    echo -e "\033[31mFail\033[0m"
fi

echo "Cleanup..."
# rm ../data/intermediate/intermediate-*
rm ../data/out/out-*
rm wc.so
rm mrcoordinator
rm mrworker
