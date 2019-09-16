#!/bin/bash

export STORAGETAPPER_CONFIG_DIR=$(pwd)/config

if [ -z "$STORAGETAPPER_ENVIRONMENT" ]; then
	export STORAGETAPPER_ENVIRONMENT=development
fi

TIMEOUT=600s

export GOTRACEBACK="crash" #produce core file on panic

if [ -z "$NOCOVER" ]; then
	COVER="-coverprofile=profile.out -covermode=atomic"
fi

#FIXME: Because of the shared state in database tests can't be run in parallel
CMD="go test -race $COVER -test.timeout $TIMEOUT"

for i in $@; do
	$CMD $TEST_PARAM $i || exit 1
	if [ -f profile.out ]; then
		cat profile.out >> coverage.out #combine coverage report for codecov.io
		rm profile.out
	fi
done
