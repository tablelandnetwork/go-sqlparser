package sqlparser

func Parse(statement string) (*AST, error) {
	yyErrorVerbose = true
	//yyDebug = 4

	if len(statement) == 0 {
		return &AST{}, nil
	}

	lexer := &Lexer{}
	lexer.input = []byte(statement)
	lexer.readByte()

	yyParse(lexer)
	if lexer.err != nil {
		return nil, lexer.err
	}

	return lexer.ast, nil
}
