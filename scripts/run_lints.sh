#!/bin/sh

set -e

for i in "$@"; do
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
		"$i" && printf "ok\t%s\n" "$i"
done
