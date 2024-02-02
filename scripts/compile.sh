#!/usr/bin/env bash

# run the test in a fresh sub-directory.
rm -rf $CODE_DIR
mkdir $CODE_DIR || exit 1
cd $CODE_DIR || exit 1

go clean
# make sure software is freshly built.
echo "Building Project"
(go build $RACE -buildmode=plugin $REPO_ROOT/src/apps/indexer/indexer.go) || exit 1
(go build $RACE -buildmode=plugin $REPO_ROOT/src/apps/wordcount/wc.go) || exit 1
(go build $RACE -buildmode=plugin $REPO_ROOT/src/apps/mtiming/mtiming.go) || exit 1
(go build $RACE -buildmode=plugin $REPO_ROOT/src/apps/rtiming/rtiming.go) || exit 1
(go build $RACE -buildmode=plugin $REPO_ROOT/src/apps/jobcount/jobcount.go) || exit 1
(go build $RACE -buildmode=plugin $REPO_ROOT/src/apps/early_exit/early_exit.go) || exit 1
(go build $RACE -buildmode=plugin $REPO_ROOT/src/apps/crash/crash.go) || exit 1
(go build $RACE -buildmode=plugin $REPO_ROOT/src/apps/nocrash/nocrash.go) || exit 1
(go build $RACE $REPO_ROOT/src/runtimes/mrcoordinator.go) || exit 1
(go build $RACE $REPO_ROOT/src/runtimes/mrworker.go) || exit 1
(go build $RACE $REPO_ROOT/src/runtimes/mrsequential.go) || exit 1

failed_any=0

echo "Project Built!"
