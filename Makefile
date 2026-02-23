.PHONY: test-unit test-integration test-all build clean

test-unit:
	go test ./pkg/thinrsmq/... -run 'TestUnit' -count=1 -v

test-integration:
	go test ./pkg/thinrsmq/... ./test/... -count=1 -v -timeout 120s

test-all: test-unit test-integration

build:
	mkdir -p bin
	go build -o bin/thinrsmq-producer ./cmd/producer
	go build -o bin/thinrsmq-consumer ./cmd/consumer
	go build -o bin/thinrsmq-admin ./cmd/admin

clean:
	rm -rf bin
