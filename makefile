.PHONY: build
build:
	go install .

.PHONY: build-lint
build-lint:
	go get -u github.com/alecthomas/gometalinter && gometalinter --install

.PHONY: lint
lint: build-lint
	gometalinter ./...

.PHONY: test
test:
	go test -v -cover ./...

