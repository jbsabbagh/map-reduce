#!/usr/bin/env bash

echo '***' Starting map parallelism test.

$TIMEOUT $CODE_DIR/mrcoordinator $DATA_DIR/pg*txt &
sleep 1

$TIMEOUT $CODE_DIR/mrworker $CODE_DIR/mtiming.so &
$TIMEOUT $CODE_DIR/mrworker $CODE_DIR/mtiming.so

NT=`cat $DATA_DIR/out/* | grep '^times-' | wc -l | sed 's/ //g'`
if [ "$NT" != "2" ]
then
  echo '---' saw "$NT" workers rather than 2
  echo '---' map parallelism test: FAIL
  failed_any=1
fi

if cat $DATA_DIR/out/* | grep '^parallel.* 2' > /dev/null
then
  echo '---' map parallelism test: PASS
else
  echo '---' map workers did not run in parallel
  echo '---' map parallelism test: FAIL
  failed_any=1
fi
