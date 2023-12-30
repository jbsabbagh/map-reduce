#!/bin/bash

start=$(date +%s)
echo "Running test..."
go build -buildmode=plugin apps/wordcount/wc.go
go run main.go wc.so data/pg*.txt
end=$(date +%s)

echo "Time elapsed: $((end-start))s"

if diff -q mr-correct mr-out-0 >/dev/null; then
    echo -e "\033[32mPass\033[0m"
else
    echo -e "\033[31mFail\033[0m"
fi


echo "Cleanup..."
rm data/intermediate/intermediate-*
rm mr-out*
