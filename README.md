# Tableland SQL Parser

This is a Go library for parsing a Tableland SQL statement as defined by [Tableland SQL Specification](https://textile.notion.site/Tableland-SQL-Specification-9493b88eac8b4dd9ad5dc76323f7f087).

It uses `goyacc` to generate a parser based on a given [grammar](./grammar.y) and a given [lexer](lexer.go).
With the parser, you can generate an AST from a SQL statement.

This is inspired on the [xwb1989/sqlparser](https://github.com/xwb1989/sqlparser), with eyes on SQLite's [grammar](https://repo.or.cz/sqlite.git/blob/HEAD:/src/parse.y) and [spec](https://www.sqlite.org/lang.html).

## Generating the parser

```bash
go run golang.org/x/tools/cmd/goyacc@master -l -o yy_parser.go grammar.y
```

## Usage

```go
ast, err := sqlparser.Parse("SELECT * FROM table WHERE c1 > c2")
if err != nil {
    panic(err)
}

ast.PrettyPrint()
```

Resulting AST:

```bash
(*sqlparser.AST)({
 Root: (*sqlparser.Select)({
  SelectColumnList: (sqlparser.SelectColumnList) (len=1 cap=1) {
   (*sqlparser.StarSelectColumn)({
    TableRef: (*sqlparser.Table)(<nil>)
   })
  },
  From: (*sqlparser.Table)({
   Name: (string) (len=5) "table"
  }),
  Where: (*sqlparser.Where)({
   Type: (string) (len=5) "where",
   Expr: (*sqlparser.CmpExpr)({
    Operator: (string) (len=1) ">",
    Left: (*sqlparser.Column)({
     Name: (string) (len=2) "c1",
     TableRef: (*sqlparser.Table)(<nil>)
    }),
    Right: (*sqlparser.Column)({
     Name: (string) (len=2) "c2",
     TableRef: (*sqlparser.Table)(<nil>)
    }),
    Escape: (sqlparser.Expr) <nil>
   })
  })
 })
})
 ```
