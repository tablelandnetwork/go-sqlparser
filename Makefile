lint:
	go run github.com/golangci/golangci-lint/cmd/golangci-lint@v1.51.0 run
.PHONY: lint

test:
	go test ./... -race
.PHONY: test

generate:
	go run golang.org/x/tools/cmd/goyacc@master -l -o yy_parser.go grammar.y
.PHONY: generate

# requires java
generate-diagrams:
	go run ebnf/main.go grammar.y | java -jar rr/rr.war -suppressebnf -color:#FFFFFF -out:diagrams.xhtml -
.PHONY: generate-diagrams