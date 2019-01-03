#!/bin/bash

set -ex

export GOPATH=$HOME/gopath
export PATH=$HOME/gopath/bin:/usr/local/go/bin:$PATH
export UBER_DATACENTER="sjc1"

/bin/sh scripts/prepare_test_env.sh

P=$GOPATH/src/github.com/uber
mkdir -p $P
cp -ap /storagetapper $P
cd $P/storagetapper

PKGS=$(find . -maxdepth 1 -type d -not -path '*/\.*'|grep -v -e vendor -e doc -e debian -e scripts -e udeploy -e go-build -e idl -e testdata -e dashboard|sort -r)

sh scripts/run_lints.sh $PKGS

make -k test-xml GO_VERSION_SETUP_DISABLED=1 RACE="-race" TEST_VERBOSITY_FLAG="-v" PHAB_COMMENT=.phabricator-comment && cp coverage.xml junit.xml /storagetapper && chmod a+rw /storagetapper/coverage.xml /storagetapper/junit.xml
