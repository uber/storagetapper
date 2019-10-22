#!/bin/sh

set -ex

GOVERSION=1.12.10
[ -f go$GOVERSION.linux-amd64.tar.gz ] || wget https://dl.google.com/go/go$GOVERSION.linux-amd64.tar.gz
echo "aaa84147433aed24e70b31da369bb6ca2859464a45de47c2a5023d8573412f6b go$GOVERSION.linux-amd64.tar.gz" | sha256sum -c -
sudo tar -xzf go$GOVERSION.linux-amd64.tar.gz -C /usr/local

export GOPATH=$HOME/gopath
export PATH=/usr/local/go/bin:$PATH
export GOROOT=/usr/local/go
curl -sfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh| sh -s -- -b "$(go env GOPATH)/bin" v1.20.0
go get github.com/tinylib/msgp

