.PHONY: build
build:
	go install .

.PHONY: lint
lint:
	golangci-lint run

.PHONY: test
test:
	go test -timeout 30s -v -cover .

