#!/usr/bin/env bash

echo '***' Starting crash test.

# generate the correct output
$CODE_DIR/mrsequential $CODE_DIR/nocrash.so $DATA_DIR/pg*txt || exit 1
sort mr-out-0 > mr-correct-crash.txt
rm -f mr-out*

rm -f mr-done
($TIMEOUT $CODE_DIR/mrcoordinator $DATA_DIR/pg*txt ; touch mr-done ) &
sleep 1

# start multiple workers
$TIMEOUT $CODE_DIR/mrworker $CODE_DIR/crash.so &

# mimic rpc.go's coordinatorSock()
SOCKNAME=/var/tmp/824-mr-`id -u`

( while [ -e $SOCKNAME -a ! -f mr-done ]
  do
    $TIMEOUT $CODE_DIR/mrworker $CODE_DIR/crash.so
    sleep 1
  done ) &

( while [ -e $SOCKNAME -a ! -f mr-done ]
  do
    $TIMEOUT $CODE_DIR/mrworker $CODE_DIR/crash.so
    sleep 1
  done ) &

while [ -e $SOCKNAME -a ! -f mr-done ]
do
  $TIMEOUT $CODE_DIR/mrworker $CODE_DIR/crash.so
  sleep 1
done

wait

rm $SOCKNAME
sort $DATA_DIR/out/* | grep . > mr-crash-all
if cmp mr-crash-all mr-correct-crash.txt
then
  echo '---' crash test: PASS
else
  echo '---' crash output is not the same as mr-correct-crash.txt
  echo '---' crash test: FAIL
  failed_any=1
fi
