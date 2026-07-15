.PHONY: all build test fmt vet check

all: check

build:
	go build ./...

test:
	go test ./...

fmt:
	gofmt -w $$(find cmd internal -name '*.go' -type f 2>/dev/null)

vet:
	go vet ./...

check: build test vet
