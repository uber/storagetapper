NAME := storagetapper
GIT_REVISION := $(shell git rev-parse --short HEAD)
TEST_TIMEOUT := 600s

PKGS := $(shell find . -maxdepth 1 -type d -not -path '*/\.*'|grep -v -e vendor -e doc -e debian -e scripts)
SRCS := $(shell find . -name "*.go" -not -path './vendor')

$(NAME): $(SRCS) vendor
	go build -ldflags "-X main.revision=$(GIT_REVISION)"

vendor:
	glide install

install: $(NAME)
	install -m 0755 -d $(DESTDIR)/usr/bin
	install -m 0550 -s $(NAME) $(DESTDIR)/usr/bin
	install -m 0750 -d $(DESTDIR)/etc/$(NAME)
	install -m 0600 config/base.yaml config/production.yaml $(DESTDIR)/etc/$(NAME)

#FIXME: Because of the shared state in database tests can't be run in parallel
unittest: $(NAME)
	for i in $(PKGS); do \
		$(shell echo $(NAME) | tr a-z A-Z)_ENVIRONMENT=development \
		$(shell echo $(NAME) | tr a-z A-Z)_CONFIG_DIR=$(shell pwd)/config \
		go test -race -cover -test.timeout $(TEST_TIMEOUT) $$i | grep -E --color -e '^FAIL' -e '^ok ' -e '$$'; \
	done

lint: $(NAME)
	CGO_ENABLED=0 gometalinter --deadline=$(TEST_TIMEOUT) --disable-all -Evet -Egolint -Egoimports -Eineffassign -Egosimple -Eerrcheck -Eunused -Edeadcode -Emisspell $(PKGS)

test: unittest lint

deb:
	dpkg-buildpackage -uc -us -b

clean:
	rm -f $(NAME)
