.PHONY: build
build:
	go install .

.PHONY: build-lint
build-lint:
	go get -u github.com/golangci/golangci-lint/cmd/golangci-lint

.PHONY: lint
lint: build-lint
	golangci-lint run -D errcheck

.PHONY: test
test:
	go test -v -cover ./...

