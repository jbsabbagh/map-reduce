#!/usr/bin/env bash

echo '***' Starting reduce parallelism test.

$TIMEOUT $CODE_DIR/mrcoordinator $DATA_DIR/pg*txt &
sleep 1

$TIMEOUT $CODE_DIR/mrworker $CODE_DIR/rtiming.so &
$TIMEOUT $CODE_DIR/mrworker $CODE_DIR/rtiming.so

NT=`cat $DATA_DIR/out/* | grep '^[a-z] 2' | wc -l | sed 's/ //g'`
if [ "$NT" -lt "2" ]
then
  echo '---' too few parallel reduces.
  echo '---' reduce parallelism test: FAIL
  failed_any=1
else
  echo '---' reduce parallelism test: PASS
fi
