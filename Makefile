lint:
	go run github.com/golangci/golangci-lint/cmd/golangci-lint@v1.46.2 run
.PHONY: lint

test:
	go test ./... -race
.PHONY: test