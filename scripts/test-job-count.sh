#!/usr/bin/env bash

echo '***' Starting job count test.


$TIMEOUT $CODE_DIR/mrcoordinator $DATA_DIR/pg*txt &
sleep 1

$TIMEOUT $CODE_DIR/mrworker $CODE_DIR/jobcount.so &
$TIMEOUT $CODE_DIR/mrworker $CODE_DIR/jobcount.so
$TIMEOUT $CODE_DIR/mrworker $CODE_DIR/jobcount.so &
$TIMEOUT $CODE_DIR/mrworker $CODE_DIR/jobcount.so

NT=`cat $DATA_DIR/out/* | awk '{print $2}'`
if [ "$NT" -eq "8" ]
then
  echo '---' job count test: PASS
else
  echo '---' map jobs ran incorrect number of times "($NT != 8)"
  echo '---' job count test: FAIL
  $failed_any=1
fi
