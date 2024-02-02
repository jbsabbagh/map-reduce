#!/usr/bin/env bash

export REPO_ROOT=$(git rev-parse --show-toplevel)
export DATA_DIR=$REPO_ROOT/data
export CODE_DIR=$REPO_ROOT/src/mr-tmp
export TIMEOUT=$"gtimeout -k 2s 30s"
export RACE=-race
export failed_any=0

# $REPO_ROOT/scripts/compile.sh

# $REPO_ROOT/scripts/test-simple-word-count.sh

# wait

# $REPO_ROOT/scripts/cleanup.sh
# $REPO_ROOT/scripts/test-indexer.sh

# wait

# $REPO_ROOT/scripts/cleanup.sh
# $REPO_ROOT/scripts/test-map-parallelism.sh

# wait

# $REPO_ROOT/scripts/cleanup.sh
# $REPO_ROOT/scripts/test-reduce-parallelism.sh

# wait

# $REPO_ROOT/scripts/cleanup.sh
# $REPO_ROOT/scripts/test-job-count.sh

# wait

# $REPO_ROOT/scripts/cleanup.sh
# $REPO_ROOT/scripts/test-early-exit.sh

# wait

$REPO_ROOT/scripts/cleanup.sh
$REPO_ROOT/scripts/test-crash.sh

# if [ $failed_any -eq 0 ]; then
#     echo '***' PASSED ALL TESTS
# else
#     echo '***' FAILED SOME TESTS
#     exit 1
# fi
