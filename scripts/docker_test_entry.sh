#!/bin/bash

set -ex

export GOPATH=$HOME/gopath
export PATH=$HOME/gopath/bin:/usr/local/go/bin:$PATH
export STORAGETAPPER_ENVIRONMENT="test"

/bin/sh scripts/prepare_test_env.sh

P=$GOPATH/src/github.com/uber
mkdir -p "$P"
cp -ap /storagetapper "$P"
cd "$P"/storagetapper

STORAGTAPPER_CONFIG_DIR=$(pwd)/config
export STORAGTAPPER_CONFIG_DIR

export GO111MODULE=on
make test
