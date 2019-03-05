.PHONY: build
build:
	go install .

.PHONY: build-lint
build-lint:
	go get -u github.com/golangci/golangci-lint/cmd/golangci-lint

.PHONY: lint
lint: build-lint
	golangci-lint run

.PHONY: test
test:
	go test -timeout 30s -v -cover ./...

