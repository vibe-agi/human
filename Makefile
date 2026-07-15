.PHONY: all build test fmt vet formal check

all: check

build:
	go build ./...

test:
	go test ./...

fmt:
	gofmt -w $$(find cmd internal -name '*.go' -type f 2>/dev/null)

vet:
	go vet ./...

formal:
	./formal/run-checks.sh

check: build test vet
