package sqlparser

func Parse(statement string) (*AST, error) {
	lexer := &Lexer{}

	yyErrorVerbose = true
	//yyDebug = 4

	lexer.input = []byte(statement)
	lexer.readByte()

	yyParse(lexer)

	if lexer.err != nil {
		return nil, lexer.err
	}

	return lexer.ast, nil
}
