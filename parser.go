package sqlparser

import "sync"

// parserPool is a pool for parser objects.
var parserPool = sync.Pool{
	New: func() interface{} {
		return &yyParserImpl{}
	},
}

var zeroParser yyParserImpl

func yyParsePooled(yylex yyLexer) int {
	parser := parserPool.Get().(*yyParserImpl)
	defer func() {
		*parser = zeroParser
		parserPool.Put(parser)
	}()
	return parser.Parse(yylex)
}

func Parse(statement string) (*AST, error) {
	//yyErrorVerbose = true
	//yyDebug = 4

	if len(statement) == 0 {
		return &AST{}, nil
	}

	lexer := &Lexer{}
	lexer.errors = make(map[int]error)
	lexer.input = []byte(statement)
	lexer.readByte()

	yyParsePooled(lexer)
	if lexer.syntaxError != nil {
		return nil, lexer.syntaxError
	}

	if len(lexer.errors) != 0 {
		lexer.ast.Errors = lexer.errors
	}
	return lexer.ast, nil
}
