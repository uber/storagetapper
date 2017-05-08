GIT_REVISION := $(shell git rev-parse --short HEAD)
TEST_TIMEOUT := 600s

PKGS := $(shell find . -maxdepth 1 -type d -not -path '*/\.*'|grep -v -e vendor -e doc -e debian)
SRCS := $(shell find . -name "*.go" -not -path './vendor')

storagetapper: $(SRCS) vendor
	go build -ldflags "-X main.revision=$(GIT_REVISION)"

vendor:
	glide install

#FIXME: Because of the shared state in database tests can't be run in parallel
unittest: storagetapper
	for i in $(PKGS); do \
		STORAGETAPPER_ENVIRONMENT=development \
		STORAGETAPPER_CONFIG_DIR=$(shell pwd)/config \
		go test -race -cover -test.timeout $(TEST_TIMEOUT) $$i | grep -E --color -e '^FAIL' -e '^ok ' -e '$$'; \
	done

lint: storagetapper
	gometalinter --deadline=$(TEST_TIMEOUT) --disable-all -Evet -Egolint -Egoimports -Eineffassign -Egosimple -Eerrcheck -Eunused -Edeadcode -Emisspell $(PKGS)

test: unittest lint

deb:
	dpkg-buildpackage -uc -us -b

clean:
	rm storagetapper
