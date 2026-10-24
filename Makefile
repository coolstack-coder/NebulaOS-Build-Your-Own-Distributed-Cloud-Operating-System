GO ?= go
BIN := bin/nebula

.PHONY: all build go web proto test race bench demo docker clean

all: build

web:
	cd web && npm ci --no-audit --no-fund && npm run build && touch dist/.gitkeep

build: web
	$(GO) build -trimpath -o $(BIN) ./cmd/nebula

# Go only, reusing whatever dashboard build is in web/dist.
go:
	$(GO) build -o $(BIN) ./cmd/nebula

proto:
	protoc -I proto --go_out=gen --go_opt=paths=source_relative \
	  --go-grpc_out=gen --go-grpc_opt=paths=source_relative proto/nebula/v1/*.proto

test:
	$(GO) vet ./...
	$(GO) test ./...

race:
	$(GO) test -race -count=3 ./...

demo: build
	./$(BIN) demo

bench: build
	./$(BIN) bench --runs 5 --out docs/BENCHMARKS.md

docker:
	docker compose up -d --build

clean:
	rm -rf bin .nebula data web/dist/assets web/dist/index.html web/dist/favicon.svg
