#!/usr/bin/env bash

#########################################################
# now indexer
# generate the correct output
$CODE_DIR/mrsequential $CODE_DIR/indexer.so $DATA_DIR/pg*txt || exit 1
sort mr-out-0 > mr-correct-indexer.txt
rm -f mr-out*

echo '***' Starting indexer test.

$TIMEOUT $CODE_DIR/mrcoordinator $DATA_DIR/pg*txt &
sleep 1

# start multiple workers
$TIMEOUT $CODE_DIR/mrworker $CODE_DIR/indexer.so &
$TIMEOUT $CODE_DIR/mrworker $CODE_DIR/indexer.so

sort $DATA_DIR/out/out* | grep . > mr-indexer-all
if cmp mr-indexer-all mr-correct-indexer.txt
then
  echo '---' indexer test: PASS
else
  echo '---' indexer output is not the same as mr-correct-indexer.txt
  echo '---' indexer test: FAIL
  $failed_any=1
fi

rm $DATA_DIR/intermediate/*
rm $DATA_DIR/out/*
