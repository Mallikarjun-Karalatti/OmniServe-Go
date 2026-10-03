.PHONY: all build run test bench web-install web-dev web-build clean help

BINARY=bin/omniserve
PORT=8081

all: build

help:
	@echo "OmniServe-Go Build and Execution Commands:"
	@echo "  make build        - Compile the OmniServe-Go gateway binary"
	@echo "  make run          - Start the gateway daemon on port $(PORT)"
	@echo "  make test         - Run test suite and high-concurrency simulation"
	@echo "  make bench        - Run dynamic batching & sync.Pool allocation benchmarks"
	@echo "  make web-install  - Install React dashboard npm dependencies"
	@echo "  make web-dev      - Run Vite observability dashboard on http://localhost:5175"
	@echo "  make web-build    - Build production assets for the frontend"
	@echo "  make clean        - Remove compiled binaries and temporary artifacts"

build:
	@mkdir -p bin
	go build -o $(BINARY) cmd/omniserve/main.go
	@echo "Built $(BINARY) successfully."

run: build
	./$(BINARY) -port=$(PORT) -batch-size=16 -batch-timeout-ms=10 -queue-capacity=2048 -workers=3

test:
	go test -v ./test/...

bench:
	go test -v -bench=. -benchmem ./test/...

web-install:
	cd web && npm install

web-dev:
	cd web && npm run dev -- --port 5175

web-build:
	cd web && npm run build

clean:
	rm -rf bin/
