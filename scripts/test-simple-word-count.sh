#!/usr/bin/env bash

#########################################################
# first word-count

# generate the correct output
$CODE_DIR/mrsequential $CODE_DIR/wc.so $DATA_DIR/pg*txt || exit 1
sort mr-out-0 > mr-correct-wc.txt
rm -f mr-out*

echo '***' Starting wc test.

$TIMEOUT $CODE_DIR/mrcoordinator $DATA_DIR/pg*txt &
pid=$!

# give the coordinator time to create the sockets.
sleep 1

# start multiple workers.
$TIMEOUT $CODE_DIR/mrworker $CODE_DIR/wc.so &
$TIMEOUT $CODE_DIR/mrworker $CODE_DIR/wc.so &
$TIMEOUT $CODE_DIR/mrworker $CODE_DIR/wc.so &

# wait for the coordinator to exit.
wait $pid

# since workers are required to exit when a job is completely finished,
# and not before, that means the job has finished.
sort $DATA_DIR/out/out* | grep . > mr-wc-all
if cmp mr-wc-all mr-correct-wc.txt
then
  echo '---' wc test: PASS
else
  echo '---' wc output is not the same as mr-correct-wc.txt
  echo '---' wc test: FAIL
  $failed_any=1
fi
