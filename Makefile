.PHONY: all test bench cover vet lint build proto tidy clean

GOFLAGS := -race
PKGS    := ./...

all: vet test build

## test: run all tests with race detector
test:
	go test $(GOFLAGS) -count=1 -timeout=60s $(PKGS)

## bench: run all benchmarks
bench:
	go test -bench=. -benchmem -run='^$$' $(PKGS)

## cover: generate HTML coverage report
cover:
	go test -coverprofile=coverage.out $(PKGS)
	go tool cover -html=coverage.out -o coverage.html
	@echo "open coverage.html"

## vet: run go vet
vet:
	go vet $(PKGS)

## lint: run golangci-lint (install: https://golangci-lint.run/usage/install/)
lint:
	golangci-lint run $(PKGS)

## build: compile all packages and the example server
build:
	go build $(PKGS)

## proto: regenerate Go code from .proto files
## Requires: protoc, protoc-gen-go, protoc-gen-go-grpc
##   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
##   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
proto:
	protoc \
		--go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		api/product/v1/product.proto

## tidy: tidy and verify the module
tidy:
	go mod tidy
	go mod verify

## clean: remove build artefacts
clean:
	go clean ./...
	rm -f coverage.out coverage.html
