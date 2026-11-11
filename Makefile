.PHONY: all build check fmt fmt-check tidy-check vet lint sqlc-diff sqlc-generate test upgrade-test clean

BIN_DIR := bin
BINARY := $(BIN_DIR)/tobitend
CMD_DIR := ./cmd/tobitend

GO ?= go
GOLANGCI_LINT ?= golangci-lint
SQLC ?= sqlc

all: check build

build:
	@mkdir -p $(BIN_DIR)
	$(GO) build -o $(BINARY) $(CMD_DIR)

check: fmt-check tidy-check vet lint sqlc-diff test

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

tidy-check:
	@echo "Checking go mod tidy..."
	$(GO) mod tidy -diff

vet:
	@echo "Running go vet..."
	$(GO) vet ./...

lint:
	@echo "Running golangci-lint..."
	$(GOLANGCI_LINT) run ./...

sqlc-diff:
	@echo "Running sqlc diff..."
	$(SQLC) diff

sqlc-generate:
	$(SQLC) generate

test:
	@echo "Running tests..."
	$(GO) test -v -race ./...

upgrade-test:
	@echo "Running database upgrade tests..."
	$(GO) test -v -race -run 'TestUpgrade' ./internal/storage/postgres/migrations/...

clean:
	rm -rf $(BIN_DIR)
