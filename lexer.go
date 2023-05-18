package sqlparser

import (
	"bytes"

	"github.com/hashicorp/go-multierror"
)

var keywords = map[string]int{
	"TRUE":       TRUE,
	"FALSE":      FALSE,
	"AND":        AND,
	"OR":         OR,
	"NOT":        NOT,
	"NULL":       NULL,
	"NONE":       NONE,
	"INTEGER":    INTEGER,
	"TEXT":       TEXT,
	"CAST":       CAST,
	"AS":         AS,
	"IS":         IS,
	"ISNULL":     ISNULL,
	"NOTNULL":    NOTNULL,
	"COLLATE":    COLLATE,
	"LIKE":       LIKE,
	"IN":         IN,
	"REGEXP":     REGEXP,
	"GLOB":       GLOB,
	"MATCH":      MATCH,
	"ESCAPE":     ESCAPE,
	"BETWEEN":    BETWEEN,
	"CASE":       CASE,
	"WHEN":       WHEN,
	"THEN":       THEN,
	"ELSE":       ELSE,
	"END":        END,
	"SELECT":     SELECT,
	"FROM":       FROM,
	"WHERE":      WHERE,
	"GROUP":      GROUP,
	"BY":         BY,
	"HAVING":     HAVING,
	"LIMIT":      LIMIT,
	"OFFSET":     OFFSET,
	"ORDER":      ORDER,
	"ASC":        ASC,
	"DESC":       DESC,
	"NULLS":      NULLS,
	"FIRST":      FIRST,
	"LAST":       LAST,
	"DISTINCT":   DISTINCT,
	"ALL":        ALL,
	"JOIN":       JOIN,
	"ON":         ON,
	"USING":      USING,
	"EXISTS":     EXISTS,
	"FILTER":     FILTER,
	"BLOB":       BLOB,
	"INT":        INT,
	"CREATE":     CREATE,
	"TABLE":      TABLE,
	"PRIMARY":    PRIMARY,
	"KEY":        KEY,
	"UNIQUE":     UNIQUE,
	"CHECK":      CHECK,
	"DEFAULT":    DEFAULT,
	"GENERATED":  GENERATED,
	"ALWAYS":     ALWAYS,
	"STORED":     STORED,
	"VIRTUAL":    VIRTUAL,
	"CONSTRAINT": CONSTRAINT,
	"INSERT":     INSERT,
	"VALUES":     VALUES,
	"INTO":       INTO,
	"DELETE":     DELETE,
	"UPDATE":     UPDATE,
	"SET":        SET,
	"GRANT":      GRANT,
	"TO":         TO,
	"REVOKE":     REVOKE,
	"CONFLICT":   CONFLICT,
	"DO":         DO,
	"NOTHING":    NOTHING,
	"CROSS":      CROSS,
	"LEFT":       LEFT,
	"NATURAL":    NATURAL,
	"OUTER":      OUTER,
	"RIGHT":      RIGHT,
	"FULL":       FULL,
	"INNER":      INNER,
	"UNION":      UNION,
	"EXCEPT":     EXCEPT,
	"INTERSECT":  INTERSECT,
	"ALTER":      ALTER,
	"RENAME":     RENAME,
	"COLUMN":     COLUMN,
	"ADD":        ADD,
	"DROP":       DROP,
}

// EOF is the end of input.
const EOF = 0

// Lexer is responsible for token generation.
type Lexer struct {
	input        []byte
	position     int
	readPosition int
	ch           byte

	literal []byte

	statementIdx int
	errors       map[int]error
	syntaxError  error

	// This is used to solve the ambigous grammar rules:
	// - expr AND expr
	// - expr BETWEEN expr AND expr
	//
	// If BETWEEN was seen, we emit a different token for AND.
	hasSeenBetween bool

	// This is used to make the NOT keyword unambigous.
	// When the NOT token is seen right after the IS token, we emit the ISNOT token (instead of of the NOT).
	lastToken int

	ast *AST

	// This is used to check if CREATE stmt has more than one primary key
	createStmtHasPrimaryKey bool
}

// AddError keeps track of errors per statement for syntatically valid statements.
func (l *Lexer) AddError(err error) {
	l.errors[l.statementIdx] = multierror.Append(l.errors[l.statementIdx], err)
}

// Error is used for syntatically not valid statements.
func (l *Lexer) Error(e string) {
	l.syntaxError = &ErrSyntaxError{YaccError: e, Position: l.position, Literal: string(l.literal)}
}

// Lex returns a token to be used in the parser.
func (l *Lexer) Lex(lval *yySymType) (token int) {
	defer func() {
		l.lastToken = token
	}()

	l.skipWhitespace()

	if l.ch == 0 {
		return EOF
	}

	if isComparison(l.ch) {
		token, literal := l.readComparison()

		l.literal = literal
		lval.bytes = literal
		return token
	}

	if isLetter(l.ch) {
		// BLOB
		if l.ch == 'X' || l.ch == 'x' {
			if l.peekByte() == '\'' {
				token, literal := l.readBlob()
				l.literal = literal
				lval.bytes = literal
				return token
			}
		}

		literal := l.readIdentifier()
		literalUpper := bytes.ToUpper(literal)

		if token, ok := keywords[string(literalUpper)]; ok {
			if token == BETWEEN {
				l.hasSeenBetween = true
			}
			if token == AND {
				if l.hasSeenBetween {
					l.hasSeenBetween = false
				} else {
					token = ANDOP
				}
			}

			if l.lastToken == IS && token == NOT {
				token = ISNOT
			}

			l.literal = literal
			lval.bytes = literal
			return token
		}

		l.literal = literal
		lval.bytes = literal
		return IDENTIFIER
	}

	if isDigit(l.ch) {
		if l.ch == '0' {
			if l.peekByte() == 'x' || l.peekByte() == 'X' {
				var buf bytes.Buffer
				buf.WriteByte('0')
				l.readByte()
				buf.WriteByte(l.ch)
				l.readByte()

				l.readDigits(16, &buf)
				if isLetter(l.ch) {
					l.literal = buf.Bytes()
					return ERROR
				}

				l.literal = buf.Bytes()
				lval.bytes = buf.Bytes()
				return HEXNUM
			}
		}

		token, literal := l.readNumber()

		l.literal = literal
		lval.bytes = literal
		return token
	}

	if l.ch == '.' {
		if isDigit(l.peekByte()) {
			var buf bytes.Buffer
			buf.WriteByte('.')
			l.readByte()
			l.readDigits(10, &buf)
			if l.ch == 'e' || l.ch == 'E' {
				l.readExpoent(&buf)
			}

			l.literal = buf.Bytes()
			lval.bytes = buf.Bytes()
			return FLOAT
		}

		l.literal = []byte{l.ch}
		l.readByte()
		return int('.')
	}

	if l.ch == '\'' {
		token, literal := l.readString()

		l.literal = literal
		lval.bytes = literal
		return token
	}

	if l.ch == '-' {
		var buf bytes.Buffer
		if l.peekByte() == '>' {
			buf.WriteByte('-')
			l.readByte()
			buf.WriteByte('>')
			l.readByte()
			if l.ch == '>' {
				buf.WriteByte('>')
				l.readByte()
				l.literal = buf.Bytes()
				return JSON_UNQUOTE_EXTRACT_OP
			}
			l.literal = buf.Bytes()
			return JSON_EXTRACT_OP
		}

		l.literal = []byte{l.ch}
		l.readByte()
		return int('-')
	}

	if l.ch == '|' {
		if l.peekByte() == '|' {
			var buf bytes.Buffer
			buf.WriteByte(l.ch)
			l.readByte()
			buf.WriteByte(l.ch)
			l.readByte()
			l.literal = buf.Bytes()
			return CONCAT
		}
		l.literal = []byte{'|'}
		l.readByte()
		return int('|')
	}

	switch ch := l.ch; ch {
	case '"', '`', '[':
		closingChar := map[byte]byte{
			'"': '"',
			'`': '`',
			'[': ']',
		}
		l.readByte() // consume opening char
		literal := l.readEnclosedIdentifier()
		if l.ch != closingChar[ch] {
			l.literal = literal
			return ERROR
		}
		literal = append([]byte{ch}, append(literal, l.ch)...)
		l.readByte() // consume closing char

		l.literal = literal
		lval.bytes = literal
		return IDENTIFIER
	}

	switch ch := l.ch; ch {
	case '(', ')', ',', '&', '+', '*', '/', '%', '~', ';':
		l.literal = []byte{ch}
		l.readByte()
		return int(ch)
	}

	l.literal = []byte{l.ch}
	return ERROR
}

func (l *Lexer) readIdentifier() []byte {
	position := l.position
	for isLetter(l.ch) || isDigit(l.ch) {
		l.readByte()
	}
	return l.input[position:l.position]
}

func (l *Lexer) readEnclosedIdentifier() []byte {
	position := l.position
	for isLetter(l.ch) || isDigit(l.ch) || l.ch == '.' {
		l.readByte()
	}
	return l.input[position:l.position]
}

func (l *Lexer) readNumber() (int, []byte) {
	var buf bytes.Buffer
	isFloat := false

	l.readDigits(10, &buf)
	if l.ch == '.' {
		isFloat = true
		buf.WriteByte(l.ch)
		l.readByte()
		l.readDigits(10, &buf)
	}

	if l.ch == 'e' || l.ch == 'E' {
		isFloat = true
		l.readExpoent(&buf)
	}

	if isFloat {
		return FLOAT, buf.Bytes()
	}

	return INTEGRAL, buf.Bytes()
}

func (l *Lexer) readDigits(base int, buf *bytes.Buffer) {
	for digitVal(l.ch) < base {
		buf.WriteByte(l.ch)
		l.readByte()
	}
}

func (l *Lexer) readExpoent(buf *bytes.Buffer) {
	buf.WriteByte(l.ch)
	l.readByte()
	if l.ch == '+' || l.ch == '-' {
		buf.WriteByte(l.ch)
		l.readByte()
	}
	l.readDigits(10, buf)
}

func (l *Lexer) readBlob() (int, []byte) {
	var buf bytes.Buffer
	l.readByte()
	l.readByte()
	for isHex(l.ch) {
		buf.WriteByte(l.ch)
		l.readByte()
	}

	if l.ch == '\'' {
		l.readByte()
		return BLOBVAL, buf.Bytes()
	}

	return ERROR, buf.Bytes()
}

func isHex(ch byte) bool {
	return '0' <= ch && ch <= '9' || 'a' <= ch && ch <= 'f' || 'A' <= ch && ch <= 'F'
}

func digitVal(ch byte) int {
	switch {
	case '0' <= ch && ch <= '9':
		return int(ch) - '0'
	case 'a' <= ch && ch <= 'f':
		return int(ch) - 'a' + 10
	case 'A' <= ch && ch <= 'F':
		return int(ch) - 'A' + 10
	}
	return 16 // larger than any legal digit val
}

// TODO(bcalza): need to account for escape sequences.
func (l *Lexer) readString() (int, []byte) {
	var literal bytes.Buffer
	literal.WriteByte(l.ch)
	l.readByte()

	for {
		if l.ch == EOF {
			return ERROR, literal.Bytes()
		}
		lastCh := l.ch
		l.readByte()

		if lastCh == '\'' {
			literal.WriteByte(lastCh)
			if l.ch == '\'' {
				l.readByte()
			} else {
				break
			}
		}

		literal.WriteByte(lastCh)
	}

	return STRING, literal.Bytes()
}

func (l *Lexer) readByte() {
	if l.readPosition >= len(l.input) {
		l.ch = 0
	} else {
		l.ch = l.input[l.readPosition]
	}

	l.position = l.readPosition
	l.readPosition++
}

func (l *Lexer) peekByte() byte {
	if l.readPosition >= len(l.input) {
		return 0
	}

	return l.input[l.readPosition]
}

func (l *Lexer) readComparison() (int, []byte) {
	switch l.ch {
	case '=':
		if l.peekByte() == '=' {
			var literal bytes.Buffer
			literal.WriteByte(l.ch)
			l.readByte()
			literal.WriteByte(l.ch)
			l.readByte()
			return int('='), literal.Bytes()
		}

		literal := l.ch
		l.readByte()
		return int('='), []byte{literal}
	case '<':
		if l.peekByte() == '=' {
			var literal bytes.Buffer
			literal.WriteByte(l.ch)
			l.readByte()
			literal.WriteByte(l.ch)
			l.readByte()
			return LE, literal.Bytes()
		} else if l.peekByte() == '>' {
			var literal bytes.Buffer
			literal.WriteByte(l.ch)
			l.readByte()
			literal.WriteByte(l.ch)
			l.readByte()
			return NE, literal.Bytes()
		} else if l.peekByte() == '<' {
			var literal bytes.Buffer
			literal.WriteByte(l.ch)
			l.readByte()
			literal.WriteByte(l.ch)
			l.readByte()
			return LSHIFT, literal.Bytes()
		} else {
			literal := l.ch
			l.readByte()
			return int('<'), []byte{literal}
		}
	case '>':
		if l.peekByte() == '=' {
			var literal bytes.Buffer
			literal.WriteByte(l.ch)
			l.readByte()
			literal.WriteByte(l.ch)
			l.readByte()
			return GE, literal.Bytes()
		} else if l.peekByte() == '>' {
			var literal bytes.Buffer
			literal.WriteByte(l.ch)
			l.readByte()
			literal.WriteByte(l.ch)
			l.readByte()
			return RSHIFT, literal.Bytes()
		} else {
			literal := l.ch
			l.readByte()
			return int('>'), []byte{literal}
		}
	case '!':
		if l.peekByte() == '=' {
			var literal bytes.Buffer
			literal.WriteByte(l.ch)
			l.readByte()
			literal.WriteByte(l.ch)
			l.readByte()
			return NE, literal.Bytes()
		}
	}

	return ERROR, []byte{l.ch}
}

func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.readByte()
	}
}

func isLetter(ch byte) bool {
	return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z' || ch == '_'
}

func isComparison(ch byte) bool {
	return ch == '=' || ch == '!' || ch == '<' || ch == '>'
}

func isDigit(ch byte) bool {
	return '0' <= ch && ch <= '9'
}
