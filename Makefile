NAME := storagetapper
GIT_REVISION := $(shell git rev-parse --short HEAD)

PKGS := $(shell go list ./...|sed 's+github.com/uber/storagetapper+.+g'|sort -r)
SRCS := $(shell find . -name "*.go" -not -path './vendor')

$(NAME): $(SRCS) types/format_gen.go
	go build -ldflags "-X main.revision=$(GIT_REVISION)"

install: $(NAME)
	install -m 0755 -d $(DESTDIR)/usr/bin
	install -m 0550 -s $(NAME) $(DESTDIR)/usr/bin
	install -m 0750 -d $(DESTDIR)/etc/$(NAME)
	install -m 0600 config/base.yaml config/production.yaml $(DESTDIR)/etc/$(NAME)

types/format_gen.go:
	go generate ./types

unittest: $(NAME)
	sh scripts/run_tests.sh $(PKGS)

lint: $(NAME)
	sh scripts/run_lints.sh $(PKGS)

shellcheck: $(NAME)
	shellcheck scripts/*.sh

bench: $(NAME)
	sh scripts/run_benchmarks.sh $(PKGS)

test: unittest lint shellcheck

deb:
	dpkg-buildpackage -uc -us -b

clean:
	rm -f $(NAME)

docker-image:
	docker build -f Dockerfile.test -t uber/storagetapper_test .

docker-test: docker-image
	-docker rm -f $(shell cat /tmp/s3server.cid)
	rm -f /tmp/s3server.cid
	docker run --cidfile=/tmp/s3server.cid -d -p 8000:8000 scality/s3server
	docker run --cap-add sys_nice --cap-add net_admin --cap-add ipc_lock -v $(shell pwd):/storagetapper uber/storagetapper_test /bin/bash /docker_test_entry.sh

test-env: docker-image
	docker run --cap-add sys_nice --cap-add net_admin --cap-add ipc_lock -it -v $(shell pwd):/storagetapper -e "GOPATH=/root/gopath" -w /storagetapper/ uber/storagetapper_test
