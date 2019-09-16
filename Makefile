NAME := storagetapper
GIT_REVISION := $(shell git rev-parse --short HEAD)

PKGS := $(shell find . -maxdepth 1 -type d -not -path '*/\.*'|grep -v -e vendor -e doc -e debian -e scripts|sort -r)
SRCS := $(shell find . -name "*.go" -not -path './vendor')

$(NAME): $(SRCS) types/format_gen.go vendor
	go build -ldflags "-X main.revision=$(GIT_REVISION)"

glide.lock: glide.yaml
	glide update

vendor: glide.lock
	glide install

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

bench: $(NAME)
	sh scripts/run_benchmarks.sh $(PKGS)

test: unittest lint

deb:
	dpkg-buildpackage -uc -us -b

clean:
	rm -f $(NAME)

docker-image: vendor
	docker build -f Dockerfile.test -t uber/storagetapper .

docker-test: docker-image
	-docker rm -f $(shell cat /tmp/s3server.cid)
	rm -f /tmp/s3server.cid
	docker run --cidfile=/tmp/s3server.cid -d -p 8000:8000 scality/s3server
	docker run --cap-add net_admin --cap-add ipc_lock -v $(shell pwd):/storagetapper uber/storagetapper /bin/bash /docker_test_entry.sh

test-env: docker-image
	docker run --cap-add net_admin --cap-add ipc_lock -it -v ~/Uber:/Uber -v $(shell printenv GOPATH)/src:/root/gopath/src -e "GOPATH=/gocode" -w /root/gopath/src/github.com/uber/storagetapper/ uber/storagetapper
