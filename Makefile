NAME := storagetapper
GIT_REVISION := $(shell git rev-parse --short HEAD)

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

unittest: $(NAME)
	sh scripts/run_tests.sh $(PKGS)

lint: $(NAME)
	sh scripts/run_lints.sh $(PKGS)

test: unittest lint

deb:
	dpkg-buildpackage -uc -us -b

clean:
	rm -f $(NAME)
