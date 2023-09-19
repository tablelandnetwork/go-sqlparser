lint:
	go run github.com/golangci/golangci-lint/cmd/golangci-lint@v1.51.0 run
.PHONY: lint

test:
	go test $(go list ./... | grep -v cmd) -race
.PHONY: test

generate:
	go run golang.org/x/tools/cmd/goyacc@master -l -o yy_parser.go grammar.y
.PHONY: generate

# requires java
generate-diagrams:
	go run ebnf/main.go grammar.y | java -jar rr/rr.war -suppressebnf -color:#FFFFFF -out:diagrams.xhtml -
.PHONY: generate-diagrams


# go get -u -v github.com/OneOfOne/struct2ts/...
types:
	struct2ts --interface --no-helpers \
	sqlparser.CreateTable \
	sqlparser.ColumnConstraintPrimaryKey \
	sqlparser.ColumnConstraintNotNull \
	sqlparser.ColumnConstraintUnique \
	sqlparser.ColumnConstraintCheck \
	sqlparser.ColumnConstraintDefault \
	sqlparser.ColumnConstraintGenerated \
	sqlparser.TableConstraintPrimaryKey \
	sqlparser.TableConstraintUnique \
	sqlparser.TableConstraintCheck \
	> js/go-types.d.ts
	echo "export type ColumnConstraint = ColumnConstraintPrimaryKey | ColumnConstraintNotNull | ColumnConstraintUnique | ColumnConstraintCheck | ColumnConstraintDefault | ColumnConstraintGenerated;" >> js/go-types.d.ts
	echo "export type TableConstraint = TableConstraintPrimaryKey | TableConstraintUnique | TableConstraintCheck;" >> js/go-types.d.ts
.PHONY: types