package sqlparser

type parser struct {
	l *Lexer
}

func NewParser() *parser {
	return &parser{
		l: &Lexer{},
	}
}

func (p *parser) Parse(sql string) (*AST, error) {
	yyErrorVerbose = true
	//yyDebug = 4

	p.l.input = []byte(sql)
	p.l.readByte()

	yyParse(p.l)

	if p.l.err != nil {
		return nil, p.l.err
	}

	return p.l.ast, nil
}
