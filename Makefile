BIN := bin/orderfence

.PHONY: build test race run report clean

build:
	go build -o $(BIN) ./cmd/orderfence

test:
	go test ./...

race:
	go test -race ./...

run: build
	./$(BIN) run

report: build
	@mkdir -p docs
	./$(BIN) run all -out docs/report.md

clean:
	rm -rf bin
