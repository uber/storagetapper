#!/bin/bash

set -e

TIMEOUT=300s

for i in $@; do
	CGO_ENABLED=0 golangci-lint run --skip-files format_gen --disable-all \
		-Egofmt \
		-Egovet \
		-Egolint \
		-Egoimports \
		-Eineffassign \
		-Eerrcheck \
		-Edeadcode \
		-Emisspell \
		-Egocyclo \
		-Estaticcheck \
		-Egosimple \
		-Estructcheck \
		-Etypecheck \
		-Eunused \
		-Evarcheck \
		-Eunconvert \
		-Emaligned \
		-Eprealloc \
		-Estylecheck \
		$i && echo "ok\t$i"
done
