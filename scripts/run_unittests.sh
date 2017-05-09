#!/bin/bash
#set -x
export STORAGETAPPER_CONFIG_DIR=$(pwd)/config
export STORAGETAPPER_ENVIRONMENT=development

TIMEOUT=300s

export GOTRACEBACK="crash" #produce core file on panic

for CMD in "go test -race -cover -test.timeout $TIMEOUT $2 $3 $4" "gometalinter --deadline=$TIMEOUT --disable-all -Evet -Egolint -Egoimports -Eineffassign -Egosimple -Eerrcheck -Eunused -Edeadcode -Emisspell"; do
#for CMD in "go test -race -cover -test.timeout $TIMEOUT $2 $3"; do
	echo "Running: $CMD"
	if [ -z "$1" ]; then
		for i in `find . -maxdepth 1 -type d -not -path '*/\.*' |grep -v -e vendor`; do $CMD $i; done | grep -E --color -e 'FAIL' -e 'ok ' -e '$'
	else
		$CMD ./$1
	fi
done
