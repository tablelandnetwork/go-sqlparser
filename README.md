# Tableland SQL Parser

This is a Go library for parsing a Tableland SQL statement.

- It uses `goyacc` to generate a parser based on a given [grammar](./grammar.y) and a given [lexer](lexer.go).
With the parser, you can generate an AST from a SQL statement.

- This is highly inspired/based on the famous [xwb1989/sqlparser](https://github.com/xwb1989/sqlparser) and SQLite's [grammar](https://repo.or.cz/sqlite.git/blob/HEAD:/src/parse.y) and [spec](https://www.sqlite.org/lang.html).

- note: this is very experimental. Right now it only handles the `expr` syntax as shown in the Syntax Diagrams.

## Generating the parser

```go
go run golang.org/x/tools/cmd/goyacc@master -l -o yy_parser.go grammar.y
```

## Usage

```go
parser := NewParser()

ast, err := parser.Parse("c1 = 2 * c2 AND c3 IS NOT NULL")
if err != nil {
    panic(err)
}

ast.PrettyPrint()
```

Resulting AST:

```bash
 Root: (*sqlparser.AndExpr)({
  Left: (*sqlparser.CmpExpr)({
   Operator: (string) (len=1) "=",
   Left: (*sqlparser.Column)({
    Name: (string) (len=2) "c1",
    TableRef: (*sqlparser.Table)(<nil>)
   }),
   Right: (*sqlparser.BinaryExpr)({
    Operator: (string) (len=1) "*",
    Left: (*sqlparser.Value)({
     Type: (sqlparser.ValueType) 1,
     Value: ([]uint8) (len=1) {
      00000000  32                                                |2|
     }
    }),
    Right: (*sqlparser.Column)({
     Name: (string) (len=2) "c2",
     TableRef: (*sqlparser.Table)(<nil>)
    })
   }),
   Escape: (sqlparser.Expr) <nil>
  }),
  Right: (*sqlparser.IsExpr)({
   Left: (*sqlparser.Column)({
    Name: (string) (len=2) "c3",
    TableRef: (*sqlparser.Table)(<nil>)
   }),
   Right: (*sqlparser.NotExpr)({
    Expr: (*sqlparser.NullValue)({
    })
   })
  })
 })
 ```

## Syntax Diagrams

### Expr

```mermaid
flowchart LR;
    S("·") --> N1(literal-value) --> E("·");
    S("·") --> N2(table-name) --> N3(.) --> N4(column-name) --> E("·");
    S("·") --> N5(unary-operator) --> N6(expr) --> E("·");
    S("·") --> N8(expr) --> N9(binary-operator) --> N10(expr) --> E("·");
    S("·") --> N11("(") --> N12(expr) --> N13(")") --> E("·");
    N12(expr) --> N14(,) --> N12(expr);
    S("·") --> N15(CAST) --> N16("(") --> N21(expr) --> N17(AS) --> N18(TYPE) --> N20(")") --> E("·");
    S("·") --> N22(expr) --> N23(COLLATE) --> N24(collation-name) --> E("·");
    S("·") --> N25(expr) --> N26(NOT) --> N27(LIKE) --> N28(expr) --> E("·");
    N25(expr)  --> N27(LIKE);
    N27(LIKE) --> N28(expr) --> N29(ESCAPE) --> N30(exp) --> E("·");
    N25(expr)  --> N31(GLOB) --> N34(expr) --> E("·");
    N25(expr)  --> N32(REGEXP) --> N34(expr) --> E("·");
    N25(expr)  --> N33(MATCH) --> N34(expr) --> E("·");
    S("·") --> N35(expr) --> ISNULL(ISNULL) --> E("·");
    N35(expr) --> NOTNULL(NOTNULL) --> E("·");
    N35(expr) --> NOT(NOT) --> NULL(NULL) --> E("·");
    S("·") --> N36(expr) --> IS(IS) --> N37(NOT) --> N38(expr) --> E("·");
    IS(IS) -->  N38(expr);
    S("·") --> N39(expr) --> N40(NOT) --> BETWEEN(BETWEEN) --> N41(expr) --> AND(AND) --> 42(expr) --> E("·");
    N39(expr) --> BETWEEN(BETWEEN);
    S("·") --> CASE(CASE) --> N43(expr) --> WHEN(WHEN) --> N44(expr) --> THEN(THEN) --> N45(expr) --> ELSE(ELSE) --> N46(expr) --> END(END)--> E("·");
    CASE(CASE) --> WHEN(WHEN);
    N45(expr) --> WHEN(WHEN);
    N45(expr) --> END(END);
```
