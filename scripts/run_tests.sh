#!/bin/bash

export STORAGETAPPER_CONFIG_DIR=$(pwd)/config
export STORAGETAPPER_ENVIRONMENT=development

TIMEOUT=300s

export GOTRACEBACK="crash" #produce core file on panic

#FIXME: Because of the shared state in database tests can't be run in parallel
CMD="go test -race -coverprofile=profile.out -covermode=atomic -test.timeout $TIMEOUT"

for i in $@; do
	$CMD $i || exit 1
	if [ -f profile.out ]; then
		cat profile.out >> coverage.txt #combine coverage report for codecov.io
		rm profile.out
	fi
done
