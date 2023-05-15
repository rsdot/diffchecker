.PHONY: build diffchecker test lint version

# Ensure GOPATH is set before running build process.
ifeq "$(GOPATH)" ""
	$(error Please set the environment variable GOPATH before running `make`)
endif

CURDIR := $(shell pwd)
path_to_add := $(addsuffix /bin,$(subst :,/bin:,$(GOPATH)))
export PATH := $(path_to_add):$(PATH)

GO       := GO111MODULE=on go
GOTEST   := CGO_ENABLED=1 $(GO) test -p 3
PACKAGES := $$(go list ./... | grep -vE 'vendor')
FILES    := $$(find . -name '*.go' -type f | grep -vE 'vendor')
BINARY	 := diffchecker

define run_unit_test
	@echo "running unit test for packages:" $(1)
	@export log_level=error; \
	$(GOTEST) -cover $(1)
endef

build: version check diffchecker

version:
	$(GO) version

diffchecker:
	$(GO) build -o bin/$(BINARY) .

test: version
	rm -rf /tmp/output
	$(call run_unit_test,$(PACKAGES))

integration_test: diffchecker
	@which bin/$(BINARY)
	test/run.sh

fmt:
	@echo "gofmt (simplify)"
	@gofmt -s -l -w $(FILES) 2>&1 | awk '{print} END{if(NR>0) {exit 1}}'

vet:
	@echo "vet"
	@$(GO) vet -composites=false $(PACKAGES)

lint:
	@echo "lint"
	@golangci-lint run

check: fmt vet lint

tidy:
	@$(GO) mod tidy

remove: # remove previous build binary
	rm -rf ./bin

clean: remove tidy
