.PHONY: dep importer vendor-update docker importercrossbuild install test

DEP := $(shell command -v dep 2>/dev/null)
PROTOC := $(shell command -v protoc 2>/dev/null)
VERSION := $(shell git describe --tags 2> /dev/null || echo unknown)
IDENTIFIER := $(VERSION)-$(GOOS)-$(GOARCH)
CLONE_URL=github.com/travisturner/importer
PKGS := $(shell cd $(GOPATH)/src/$(CLONE_URL); go list ./... | grep -v vendor)
BUILD_TIME=`date -u +%FT%T%z`
LDFLAGS=-ldflags "-X github.com/travisturner/importer/cmd.Version=$(VERSION) -X github.com/travisturner/importer/cmd.BuildTime=$(BUILD_TIME)"

default: test importer

$(GOPATH)/bin:
	mkdir $(GOPATH)/bin

dep: $(GOPATH)/bin
	go get -u github.com/golang/dep/cmd/dep

vendor: Gopkg.toml
ifndef DEP
	make dep
endif
	dep ensure
	touch vendor

Gopkg.lock: dep Gopkg.toml
	dep ensure

test: vendor
	go test $(PKGS) $(TESTFLAGS) ./...

importer: vendor
	go build $(LDFLAGS) $(FLAGS) $(CLONE_URL)/cmd/importer

crossbuild: vendor
	mkdir -p build/importer-$(IDENTIFIER)
	make importer FLAGS="-o build/importer-$(IDENTIFIER)/importer"

install: vendor
	go install $(LDFLAGS) $(FLAGS) $(CLONE_URL)/cmd/importer
