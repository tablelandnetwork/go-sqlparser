# Tableland SQL Parser

[![Review](https://github.com/tablelandnetwork/sqlparser/actions/workflows/review.yml/badge.svg)](https://github.com/tablelandnetwork/sqlparser/actions/workflows/review.yml)
[![Test](https://github.com/tablelandnetwork/sqlparser/actions/workflows/test.yml/badge.svg)](https://github.com/tablelandnetwork/sqlparser/actions/workflows/test.yml)
[![Release](https://img.shields.io/github/release/tablelandnetwork/sqlparser.svg)](https://github.com/tablelandnetwork/sqlparser/releases/latest)
[![standard-readme compliant](https://img.shields.io/badge/standard--readme-OK-green.svg)](https://github.com/RichardLitt/standard-readme)

> Go library for parsing Tableland-compliant SQL

# Table of Contents

- [Tableland SQL Parser](#tableland-sql-parser)
- [Table of Contents](#table-of-contents)
- [Background](#background)
- [Usage](#usage)
- [Feedback](#feedback)
- [Contributing](#contributing)
- [License](#license)

# Background

This is a Go library for parsing a Tableland SQL statement as defined by [Tableland SQL Specification](https://textile.notion.site/Tableland-SQL-Specification-9493b88eac8b4dd9ad5dc76323f7f087).

It uses `goyacc` to generate a parser based on a given [grammar](./grammar.y) and a given [lexer](lexer.go).
With the parser, you can generate an AST from a SQL statement.

This is inspired on the [xwb1989/sqlparser](https://github.com/xwb1989/sqlparser), with eyes on SQLite's [grammar](https://repo.or.cz/sqlite.git/blob/HEAD:/src/parse.y) and [spec](https://www.sqlite.org/lang.html).

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

# Contributing

To get started clone this repo.

## Generating the parser

```bash
go run golang.org/x/tools/cmd/goyacc@master -l -o yy_parser.go grammar.y
```

## Generating syntax diagrams

```bash
make generate-diagrams 
```

Requires Java 8 (or higher).

# Feedback

Reach out with feedback and ideas:

- [twitter.com/tableland\_\_](https://twitter.com/tableland__)
- [Create a new issue](https://github.com/tablelandnetwork/sqlparser/issues)

# License

MIT AND Apache-2.0, © 2021-2022 Tableland Network Contributors