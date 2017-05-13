#!/bin/bash

TIMEOUT=300s

CGO_ENABLED=0 gometalinter --deadline=$TIMEOUT --disable-all -Evet -Egolint -Egoimports -Eineffassign -Egosimple -Eerrcheck -Eunused -Edeadcode -Emisspell -Egocyclo --cyclo-over=15 $@
