lint:
	go run github.com/golangci/golangci-lint/cmd/golangci-lint@v1.46.2 run
.PHONY: lint

test:
	go test ./... -race
.PHONY: test

generate:
	go run golang.org/x/tools/cmd/goyacc@master -l -o yy_parser.go grammar.y
.PHONY: generate