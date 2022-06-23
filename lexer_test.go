package sqlparser

import (
	"testing"
)

func TestLexer(t *testing.T) {
	input := `
		_hello 123 1.3 0.1232 'string'
		0xAF273 X'AF273' x'AF273' true false 
		null TRUE FALSE NULL NONE 
		INTEGER NUMERIC REAL TEXT CAST 
		AS ( , ) AND 
		OR NOT = != > 
		< >= <= <> ->
		->> || LIKE IS ISNULL
		NOTNULL IN regexp GLOB MATCH 
		ESCAPE COLLATE . & | + * / % ~ ;
		<< >> BETWEEN AND
		CASE WHEN THEN ELSE END
		SELECT FROM WHERE GROUP BY 
		HAVING LIMIT OFFSET ORDER ASC DESC NULLS FIRST LAST 
		DISTINCT ALL JOIN ON USING EXISTS IS NOT FILTER
		CREATE TABLE INT BLOB ANY PRIMARY KEY UNIQUE CHECK DEFAULT GENERATED ALWAYS STORED VIRTUAL CONSTRAINT
		INSERT INTO VALUES DELETE
	`
	expTokens := []int{
		IDENTIFIER, INTEGRAL, FLOAT, FLOAT, STRING,
		HEXNUM, BLOBVAL, BLOBVAL, TRUE, FALSE,
		NULL, TRUE, FALSE, NULL, NONE,
		INTEGER, NUMERIC, REAL, TEXT, CAST,
		AS, '(', ',', ')', ANDOP,
		OR, NOT, '=', NE, '>',
		'<', GE, LE, NE, JSON_EXTRACT_OP,
		JSON_UNQUOTE_EXTRACT_OP, CONCAT, LIKE, IS, ISNULL,
		NOTNULL, IN, REGEXP, GLOB, MATCH,
		ESCAPE, COLLATE, '.', '&', '|',
		'+', '*', '/', '%', '~', ';',
		LSHIFT, RSHIFT, BETWEEN, AND,
		CASE, WHEN, THEN, ELSE, END,
		SELECT, FROM, WHERE, GROUP, BY,
		HAVING, LIMIT, OFFSET, ORDER, ASC, DESC, NULLS, FIRST, LAST,
		DISTINCT, ALL, JOIN, ON, USING, EXISTS, IS, ISNOT, FILTER,
		CREATE, TABLE, INT, BLOB, ANY, PRIMARY, KEY, UNIQUE, CHECK, DEFAULT, GENERATED, ALWAYS, STORED, VIRTUAL, CONSTRAINT,
		INSERT, INTO, VALUES, DELETE,
	}

	lval := &yySymType{}

	lexer := &Lexer{}
	lexer.input = []byte(input)
	lexer.readByte()

	token, i := lexer.Lex(lval), 0
	for token != EOF {
		if token != expTokens[i] {
			t.Fatalf("expected %d, got %d, at index %d", expTokens[i], token, i)
		}
		token, i = lexer.Lex(lval), i+1
	}
}
