package sqlparser

func Parse(statement string) (*AST, error) {
	yyErrorVerbose = true
	//yyDebug = 4

	if len(statement) == 0 {
		return &AST{}, nil
	}

	lexer := &Lexer{}
	lexer.errors = make(map[int][]error)
	lexer.input = []byte(statement)
	lexer.readByte()

	yyParse(lexer)
	if lexer.syntaxError != nil {
		return nil, lexer.syntaxError
	}

	if len(lexer.errors) != 0 {
		lexer.ast.Errors = lexer.errors
	}
	return lexer.ast, nil
}
