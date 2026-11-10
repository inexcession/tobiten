.PHONY: all build check fmt fmt-check vet lint test clean

BIN_DIR := bin
BINARY := $(BIN_DIR)/tobitend
CMD_DIR := ./cmd/tobitend

GO ?= go
GOLANGCI_LINT ?= golangci-lint

all: check build

build:
	@mkdir -p $(BIN_DIR)
	$(GO) build -o $(BINARY) $(CMD_DIR)

check: fmt-check vet lint test

fmt:
	$(GO) fmt ./...

fmt-check:
	@echo "Checking formatting..."
	@unformatted=$$(gofmt -l $$(find . -name '*.go' -not -path './untracked/*' -not -path './.*')); \
	if [ -n "$$unformatted" ]; then \
		echo "The following files are not formatted:"; \
		echo "$$unformatted"; \
		exit 1; \
	fi

vet:
	@echo "Running go vet..."
	$(GO) vet ./...

lint:
	@echo "Running golangci-lint..."
	$(GOLANGCI_LINT) run ./...

test:
	@echo "Running tests..."
	$(GO) test -v -race ./...

clean:
	rm -rf $(BIN_DIR)
