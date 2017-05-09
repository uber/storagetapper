GIT_REVISION := $(shell git rev-parse --short HEAD)

storagetapper:
	go build -ldflags "-X main.revision=$(GIT_REVISION)"
