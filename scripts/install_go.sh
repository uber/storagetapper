#!/bin/sh

set -ex

GOVERSION=1.17.3
[ -f go$GOVERSION.linux-amd64.tar.gz ] || wget https://dl.google.com/go/go$GOVERSION.linux-amd64.tar.gz
echo "550f9845451c0c94be679faf116291e7807a8d78b43149f9506c1b15eb89008c go$GOVERSION.linux-amd64.tar.gz" | sha256sum -c -
sudo tar -xzf go$GOVERSION.linux-amd64.tar.gz -C /usr/local

export GOPATH=$HOME/gopath
export PATH=/usr/local/go/bin:$PATH
export GOROOT=/usr/local/go
curl -sfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh| sh -s -- -b "$(go env GOPATH)/bin" v1.43.0
go install github.com/tinylib/msgp@v1.1.6
