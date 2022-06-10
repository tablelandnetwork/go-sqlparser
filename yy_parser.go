// Code generated by goyacc -l -o yy_parser.go grammar.y. DO NOT EDIT.
package sqlparser

import __yyfmt__ "fmt"

const MaxColumnNameLength = 64

type yySymType struct {
	yys           int
	bool          bool
	string        string
	bytes         []byte
	expr          Expr
	exprs         Exprs
	column        *Column
	table         *Table
	convertType   ConvertType
	when          *When
	whens         []*When
	resultColumn  ResultColumn
	resultColumns ResultColumns
	selectStmt    *Select
}

const IDENTIFIER = 57346
const STRING = 57347
const INTEGRAL = 57348
const HEXNUM = 57349
const FLOAT = 57350
const BLOB = 57351
const ERROR = 57352
const TRUE = 57353
const FALSE = 57354
const NULL = 57355
const AND = 57356
const NONE = 57357
const INTEGER = 57358
const NUMERIC = 57359
const REAL = 57360
const TEXT = 57361
const CAST = 57362
const AS = 57363
const CASE = 57364
const WHEN = 57365
const THEN = 57366
const ELSE = 57367
const END = 57368
const SELECT = 57369
const FROM = 57370
const WHERE = 57371
const OR = 57372
const ANDOP = 57373
const NOT = 57374
const IS = 57375
const MATCH = 57376
const GLOB = 57377
const REGEXP = 57378
const LIKE = 57379
const BETWEEN = 57380
const IN = 57381
const ISNULL = 57382
const NOTNULL = 57383
const NE = 57384
const LE = 57385
const GE = 57386
const INEQUALITY = 57387
const ESCAPE = 57388
const LSHIFT = 57389
const RSHIFT = 57390
const CONCAT = 57391
const JSON_EXTRACT_OP = 57392
const JSON_UNQUOTE_EXTRACT_OP = 57393
const COLLATE = 57394
const UNARY = 57395

var yyToknames = [...]string{
	"$end",
	"error",
	"$unk",
	"IDENTIFIER",
	"STRING",
	"INTEGRAL",
	"HEXNUM",
	"FLOAT",
	"BLOB",
	"ERROR",
	"TRUE",
	"FALSE",
	"NULL",
	"AND",
	"'('",
	"','",
	"')'",
	"'.'",
	"NONE",
	"INTEGER",
	"NUMERIC",
	"REAL",
	"TEXT",
	"CAST",
	"AS",
	"CASE",
	"WHEN",
	"THEN",
	"ELSE",
	"END",
	"SELECT",
	"FROM",
	"WHERE",
	"OR",
	"ANDOP",
	"NOT",
	"IS",
	"MATCH",
	"GLOB",
	"REGEXP",
	"LIKE",
	"BETWEEN",
	"IN",
	"ISNULL",
	"NOTNULL",
	"NE",
	"'='",
	"'<'",
	"'>'",
	"LE",
	"GE",
	"INEQUALITY",
	"ESCAPE",
	"'&'",
	"'|'",
	"LSHIFT",
	"RSHIFT",
	"'+'",
	"'-'",
	"'*'",
	"'/'",
	"'%'",
	"CONCAT",
	"JSON_EXTRACT_OP",
	"JSON_UNQUOTE_EXTRACT_OP",
	"COLLATE",
	"'~'",
	"UNARY",
}

var yyStatenames = [...]string{}

const yyEofCode = 1
const yyErrCode = 2
const yyInitialStackSize = 16

var yyExca = [...]int8{
	-1, 1,
	1, -1,
	-2, 0,
	-1, 17,
	18, 13,
	-2, 56,
}

const yyPrivate = 57344

const yyLast = 505

var yyAct = [...]uint8{
	7, 114, 52, 119, 10, 32, 33, 34, 39, 40,
	41, 52, 69, 71, 72, 74, 76, 35, 36, 37,
	38, 30, 31, 32, 33, 34, 39, 40, 41, 52,
	3, 81, 82, 83, 84, 85, 86, 87, 88, 89,
	90, 91, 92, 93, 94, 95, 96, 97, 98, 111,
	132, 115, 106, 60, 61, 62, 63, 53, 120, 35,
	36, 37, 38, 30, 31, 32, 33, 34, 39, 40,
	41, 52, 112, 110, 60, 61, 62, 63, 118, 5,
	35, 36, 37, 38, 30, 31, 32, 33, 34, 39,
	40, 41, 52, 39, 40, 41, 52, 28, 68, 115,
	121, 125, 142, 100, 77, 109, 66, 67, 80, 117,
	116, 111, 108, 27, 107, 124, 126, 110, 127, 79,
	129, 130, 1, 131, 66, 67, 133, 54, 103, 102,
	101, 104, 105, 113, 135, 141, 46, 45, 50, 47,
	59, 58, 57, 64, 65, 4, 48, 49, 56, 55,
	60, 61, 62, 63, 29, 51, 35, 36, 37, 38,
	30, 31, 32, 33, 34, 39, 40, 41, 52, 134,
	136, 139, 140, 138, 137, 46, 45, 50, 47, 59,
	58, 57, 64, 65, 44, 48, 49, 56, 55, 60,
	61, 62, 63, 43, 42, 35, 36, 37, 38, 30,
	31, 32, 33, 34, 39, 40, 41, 52, 128, 75,
	123, 73, 16, 9, 2, 0, 0, 46, 45, 50,
	47, 59, 58, 57, 64, 65, 0, 48, 49, 56,
	55, 60, 61, 62, 63, 122, 0, 35, 36, 37,
	38, 30, 31, 32, 33, 34, 39, 40, 41, 52,
	0, 0, 0, 0, 0, 46, 45, 50, 47, 59,
	58, 57, 64, 65, 0, 48, 49, 56, 55, 60,
	61, 62, 63, 0, 0, 35, 36, 37, 38, 30,
	31, 32, 33, 34, 39, 40, 41, 52, 46, 45,
	50, 47, 59, 58, 57, 64, 65, 0, 48, 49,
	56, 55, 60, 61, 62, 63, 0, 0, 35, 36,
	37, 38, 30, 31, 32, 33, 34, 39, 40, 41,
	52, 45, 50, 47, 59, 58, 57, 64, 65, 0,
	48, 49, 56, 55, 60, 61, 62, 63, 0, 0,
	35, 36, 37, 38, 30, 31, 32, 33, 34, 39,
	40, 41, 52, 50, 47, 59, 58, 57, 64, 65,
	0, 48, 49, 56, 55, 60, 61, 62, 63, 0,
	0, 35, 36, 37, 38, 30, 31, 32, 33, 34,
	39, 40, 41, 52, 17, 18, 19, 22, 20, 21,
	0, 23, 24, 25, 70, 15, 0, 0, 8, 0,
	0, 0, 0, 0, 26, 0, 14, 0, 0, 0,
	17, 18, 19, 22, 20, 21, 99, 23, 24, 25,
	0, 15, 78, 8, 0, 0, 0, 0, 0, 0,
	26, 0, 14, 0, 0, 0, 0, 0, 12, 11,
	0, 17, 18, 19, 22, 20, 21, 13, 23, 24,
	25, 0, 15, 0, 0, 0, 0, 0, 0, 0,
	0, 26, 0, 14, 12, 11, 6, 0, 0, 0,
	0, 0, 0, 13, 30, 31, 32, 33, 34, 39,
	40, 41, 52, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 12, 11, 0, 0, 0,
	0, 0, 0, 0, 13,
}

var yyPact = [...]int16{
	-1, -1000, -1000, 406, 81, -1000, -1000, 102, 80, -1000,
	-1000, 437, 437, 437, 437, 437, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, 89, 115, 406, -1000,
	437, 437, 437, 437, 437, 437, 437, 437, 437, 437,
	437, 437, 437, 437, 437, 437, 437, 380, -1000, -1000,
	90, 437, 110, -1000, 120, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, 45, -1000,
	54, -1000, -1000, 24, 254, 93, 254, 437, -30, -1000,
	-1000, -55, -55, 30, 30, 30, 416, 416, 416, 416,
	-64, -64, -64, 26, -37, 5, 317, 286, 26, 437,
	-1000, -1000, -1000, -1000, -1000, -1000, 221, -1000, -1000, -1000,
	-1000, -1000, 107, 72, -1000, 437, -1000, 437, 183, 437,
	437, 317, 437, 20, -1000, 437, 141, 254, 151, 254,
	26, 26, -1000, 254, 437, 85, -1000, -1000, -1000, -1000,
	-1000, 254, -1000,
}

var yyPgo = [...]int16{
	0, 214, 0, 213, 212, 211, 210, 209, 194, 193,
	184, 155, 4, 154, 57, 79, 145, 394, 134, 1,
	133, 122,
}

var yyR1 = [...]int8{
	0, 21, 1, 16, 16, 15, 15, 15, 13, 13,
	13, 14, 14, 17, 2, 2, 2, 2, 2, 2,
	2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
	2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
	2, 2, 2, 2, 2, 2, 2, 2, 3, 3,
	3, 3, 3, 3, 3, 3, 12, 8, 8, 8,
	8, 8, 8, 8, 8, 9, 9, 9, 9, 10,
	10, 11, 11, 18, 18, 18, 18, 18, 4, 7,
	7, 5, 5, 19, 20, 20, 6, 6,
}

var yyR2 = [...]int8{
	0, 1, 6, 1, 3, 1, 2, 3, 0, 1,
	2, 1, 1, 1, 1, 1, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 5, 2, 2, 2, 3, 3, 3, 4,
	2, 2, 3, 5, 5, 3, 3, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	2, 1, 2, 1, 2, 1, 1, 1, 1, 1,
	2, 1, 2, 1, 1, 1, 1, 1, 6, 1,
	3, 0, 1, 4, 1, 2, 0, 2,
}

var yyChk = [...]int16{
	-1000, -21, -1, 31, -16, -15, 60, -2, -17, -3,
	-12, 59, 58, 67, 26, 15, -4, 4, 5, 6,
	8, 9, 7, 11, 12, 13, 24, 32, 16, -13,
	58, 59, 60, 61, 62, 54, 55, 56, 57, 63,
	64, 65, -8, -9, -10, 35, 34, 37, 44, 45,
	36, -11, 66, -14, 25, 47, 46, 40, 39, 38,
	48, 49, 50, 51, 41, 42, 4, 5, 18, -2,
	-17, -2, -2, -5, -2, -7, -2, 15, -17, 4,
	-15, -2, -2, -2, -2, -2, -2, -2, -2, -2,
	-2, -2, -2, -2, -2, -2, -2, -2, -2, 36,
	13, 40, 39, 38, 41, 42, -2, 4, -14, 60,
	-12, 4, 18, -20, -19, 27, 17, 16, -2, 33,
	53, -2, 14, -6, -19, 29, -2, -2, 25, -2,
	-2, -2, 30, -2, 28, -18, 19, 23, 22, 20,
	21, -2, 17,
}

var yyDef = [...]int8{
	0, -2, 1, 0, 0, 3, 5, 8, 0, 14,
	15, 0, 0, 0, 81, 0, 47, -2, 48, 49,
	50, 51, 52, 53, 54, 55, 0, 0, 0, 6,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 40, 41,
	0, 0, 0, 9, 0, 57, 58, 59, 61, 63,
	65, 66, 67, 68, 69, 71, 11, 12, 0, 33,
	0, 34, 35, 0, 82, 0, 79, 0, 0, 13,
	4, 17, 18, 19, 20, 21, 22, 23, 24, 25,
	26, 27, 28, 29, 30, 31, 36, 37, 38, 0,
	42, 60, 62, 64, 70, 72, 0, 45, 10, 7,
	16, 56, 0, 86, 84, 0, 46, 0, 0, 0,
	0, 39, 0, 0, 85, 0, 0, 80, 0, 2,
	32, 43, 44, 87, 0, 0, 73, 74, 75, 76,
	77, 83, 78,
}

var yyTok1 = [...]int8{
	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 62, 54, 3,
	15, 17, 60, 58, 16, 59, 18, 61, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	48, 47, 49, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 55, 3, 67,
}

var yyTok2 = [...]int8{
	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 19, 20, 21, 22, 23, 24, 25,
	26, 27, 28, 29, 30, 31, 32, 33, 34, 35,
	36, 37, 38, 39, 40, 41, 42, 43, 44, 45,
	46, 50, 51, 52, 53, 56, 57, 63, 64, 65,
	66, 68,
}

var yyTok3 = [...]int8{
	0,
}

var yyErrorMessages = [...]struct {
	state int
	token int
	msg   string
}{}

/*	parser for yacc output	*/

var (
	yyDebug        = 0
	yyErrorVerbose = false
)

type yyLexer interface {
	Lex(lval *yySymType) int
	Error(s string)
}

type yyParser interface {
	Parse(yyLexer) int
	Lookahead() int
}

type yyParserImpl struct {
	lval  yySymType
	stack [yyInitialStackSize]yySymType
	char  int
}

func (p *yyParserImpl) Lookahead() int {
	return p.char
}

func yyNewParser() yyParser {
	return &yyParserImpl{}
}

const yyFlag = -1000

func yyTokname(c int) string {
	if c >= 1 && c-1 < len(yyToknames) {
		if yyToknames[c-1] != "" {
			return yyToknames[c-1]
		}
	}
	return __yyfmt__.Sprintf("tok-%v", c)
}

func yyStatname(s int) string {
	if s >= 0 && s < len(yyStatenames) {
		if yyStatenames[s] != "" {
			return yyStatenames[s]
		}
	}
	return __yyfmt__.Sprintf("state-%v", s)
}

func yyErrorMessage(state, lookAhead int) string {
	const TOKSTART = 4

	if !yyErrorVerbose {
		return "syntax error"
	}

	for _, e := range yyErrorMessages {
		if e.state == state && e.token == lookAhead {
			return "syntax error: " + e.msg
		}
	}

	res := "syntax error: unexpected " + yyTokname(lookAhead)

	// To match Bison, suggest at most four expected tokens.
	expected := make([]int, 0, 4)

	// Look for shiftable tokens.
	base := int(yyPact[state])
	for tok := TOKSTART; tok-1 < len(yyToknames); tok++ {
		if n := base + tok; n >= 0 && n < yyLast && int(yyChk[int(yyAct[n])]) == tok {
			if len(expected) == cap(expected) {
				return res
			}
			expected = append(expected, tok)
		}
	}

	if yyDef[state] == -2 {
		i := 0
		for yyExca[i] != -1 || int(yyExca[i+1]) != state {
			i += 2
		}

		// Look for tokens that we accept or reduce.
		for i += 2; yyExca[i] >= 0; i += 2 {
			tok := int(yyExca[i])
			if tok < TOKSTART || yyExca[i+1] == 0 {
				continue
			}
			if len(expected) == cap(expected) {
				return res
			}
			expected = append(expected, tok)
		}

		// If the default action is to accept or reduce, give up.
		if yyExca[i+1] != 0 {
			return res
		}
	}

	for i, tok := range expected {
		if i == 0 {
			res += ", expecting "
		} else {
			res += " or "
		}
		res += yyTokname(tok)
	}
	return res
}

func yylex1(lex yyLexer, lval *yySymType) (char, token int) {
	token = 0
	char = lex.Lex(lval)
	if char <= 0 {
		token = int(yyTok1[0])
		goto out
	}
	if char < len(yyTok1) {
		token = int(yyTok1[char])
		goto out
	}
	if char >= yyPrivate {
		if char < yyPrivate+len(yyTok2) {
			token = int(yyTok2[char-yyPrivate])
			goto out
		}
	}
	for i := 0; i < len(yyTok3); i += 2 {
		token = int(yyTok3[i+0])
		if token == char {
			token = int(yyTok3[i+1])
			goto out
		}
	}

out:
	if token == 0 {
		token = int(yyTok2[1]) /* unknown char */
	}
	if yyDebug >= 3 {
		__yyfmt__.Printf("lex %s(%d)\n", yyTokname(token), uint(char))
	}
	return char, token
}

func yyParse(yylex yyLexer) int {
	return yyNewParser().Parse(yylex)
}

func (yyrcvr *yyParserImpl) Parse(yylex yyLexer) int {
	var yyn int
	var yyVAL yySymType
	var yyDollar []yySymType
	_ = yyDollar // silence set and not used
	yyS := yyrcvr.stack[:]

	Nerrs := 0   /* number of errors */
	Errflag := 0 /* error recovery flag */
	yystate := 0
	yyrcvr.char = -1
	yytoken := -1 // yyrcvr.char translated into internal numbering
	defer func() {
		// Make sure we report no lookahead when not parsing.
		yystate = -1
		yyrcvr.char = -1
		yytoken = -1
	}()
	yyp := -1
	goto yystack

ret0:
	return 0

ret1:
	return 1

yystack:
	/* put a state and value onto the stack */
	if yyDebug >= 4 {
		__yyfmt__.Printf("char %v in %v\n", yyTokname(yytoken), yyStatname(yystate))
	}

	yyp++
	if yyp >= len(yyS) {
		nyys := make([]yySymType, len(yyS)*2)
		copy(nyys, yyS)
		yyS = nyys
	}
	yyS[yyp] = yyVAL
	yyS[yyp].yys = yystate

yynewstate:
	yyn = int(yyPact[yystate])
	if yyn <= yyFlag {
		goto yydefault /* simple state */
	}
	if yyrcvr.char < 0 {
		yyrcvr.char, yytoken = yylex1(yylex, &yyrcvr.lval)
	}
	yyn += yytoken
	if yyn < 0 || yyn >= yyLast {
		goto yydefault
	}
	yyn = int(yyAct[yyn])
	if int(yyChk[yyn]) == yytoken { /* valid shift */
		yyrcvr.char = -1
		yytoken = -1
		yyVAL = yyrcvr.lval
		yystate = yyn
		if Errflag > 0 {
			Errflag--
		}
		goto yystack
	}

yydefault:
	/* default state action */
	yyn = int(yyDef[yystate])
	if yyn == -2 {
		if yyrcvr.char < 0 {
			yyrcvr.char, yytoken = yylex1(yylex, &yyrcvr.lval)
		}

		/* look through exception table */
		xi := 0
		for {
			if yyExca[xi+0] == -1 && int(yyExca[xi+1]) == yystate {
				break
			}
			xi += 2
		}
		for xi += 2; ; xi += 2 {
			yyn = int(yyExca[xi+0])
			if yyn < 0 || yyn == yytoken {
				break
			}
		}
		yyn = int(yyExca[xi+1])
		if yyn < 0 {
			goto ret0
		}
	}
	if yyn == 0 {
		/* error ... attempt to resume parsing */
		switch Errflag {
		case 0: /* brand new error */
			yylex.Error(yyErrorMessage(yystate, yytoken))
			Nerrs++
			if yyDebug >= 1 {
				__yyfmt__.Printf("%s", yyStatname(yystate))
				__yyfmt__.Printf(" saw %s\n", yyTokname(yytoken))
			}
			fallthrough

		case 1, 2: /* incompletely recovered error ... try again */
			Errflag = 3

			/* find a state where "error" is a legal shift action */
			for yyp >= 0 {
				yyn = int(yyPact[yyS[yyp].yys]) + yyErrCode
				if yyn >= 0 && yyn < yyLast {
					yystate = int(yyAct[yyn]) /* simulate a shift of "error" */
					if int(yyChk[yystate]) == yyErrCode {
						goto yystack
					}
				}

				/* the current p has no shift on "error", pop stack */
				if yyDebug >= 2 {
					__yyfmt__.Printf("error recovery pops state %d\n", yyS[yyp].yys)
				}
				yyp--
			}
			/* there is no state on the stack with an error shift ... abort */
			goto ret1

		case 3: /* no shift yet; clobber input char */
			if yyDebug >= 2 {
				__yyfmt__.Printf("error recovery discards %s\n", yyTokname(yytoken))
			}
			if yytoken == yyEofCode {
				goto ret1
			}
			yyrcvr.char = -1
			yytoken = -1
			goto yynewstate /* try again in the same state */
		}
	}

	/* reduction by production yyn */
	if yyDebug >= 2 {
		__yyfmt__.Printf("reduce %v in:\n\t%v\n", yyn, yyStatname(yystate))
	}

	yynt := yyn
	yypt := yyp
	_ = yypt // guard against "declared and not used"

	yyp -= int(yyR2[yyn])
	// yyp is now the index of $0. Perform the default action. Iff the
	// reduced production is ε, $1 is possibly out of range.
	if yyp+1 >= len(yyS) {
		nyys := make([]yySymType, len(yyS)*2)
		copy(nyys, yyS)
		yyS = nyys
	}
	yyVAL = yyS[yyp+1]

	/* consult goto table to find next state */
	yyn = int(yyR1[yyn])
	yyg := int(yyPgo[yyn])
	yyj := yyg + yyS[yyp].yys + 1

	if yyj >= yyLast {
		yystate = int(yyAct[yyg])
	} else {
		yystate = int(yyAct[yyj])
		if int(yyChk[yystate]) != -yyn {
			yystate = int(yyAct[yyg])
		}
	}
	// dummy call; replaced with literal code
	switch yynt {

	case 1:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yylex.(*Lexer).ast = &AST{yyDollar[1].selectStmt}
		}
	case 2:
		yyDollar = yyS[yypt-6 : yypt+1]
		{
			yyVAL.selectStmt = &Select{ResultColumns: yyDollar[2].resultColumns, From: yyDollar[4].table, Where: NewWhere(WhereStr, yyDollar[6].expr)}
		}
	case 3:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.resultColumns = ResultColumns{yyDollar[1].resultColumn}
		}
	case 4:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.resultColumns = append(yyDollar[1].resultColumns, yyDollar[3].resultColumn)
		}
	case 5:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.resultColumn = &StarResultColumn{}
		}
	case 6:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.resultColumn = &AliasedResultColumn{Expr: yyDollar[1].expr, As: yyDollar[2].column}
		}
	case 7:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.resultColumn = &StarResultColumn{TableRef: yyDollar[1].table}
		}
	case 8:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.column = nil
		}
	case 9:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.column = yyDollar[1].column
		}
	case 10:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.column = yyDollar[2].column
		}
	case 11:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.column = &Column{Name: string(yyDollar[1].bytes)}
		}
	case 12:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.column = &Column{Name: string(yyDollar[1].bytes)}
		}
	case 13:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.table = &Table{Name: string(yyDollar[1].bytes)}
		}
	case 14:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.expr = yyDollar[1].expr
		}
	case 15:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.expr = yyDollar[1].column
		}
	case 16:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyDollar[3].column.TableRef = yyDollar[1].table
			yyVAL.expr = yyDollar[3].column
		}
	case 17:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.expr = &BinaryExpr{Left: yyDollar[1].expr, Operator: PlusStr, Right: yyDollar[3].expr}
		}
	case 18:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.expr = &BinaryExpr{Left: yyDollar[1].expr, Operator: MinusStr, Right: yyDollar[3].expr}
		}
	case 19:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.expr = &BinaryExpr{Left: yyDollar[1].expr, Operator: MultStr, Right: yyDollar[3].expr}
		}
	case 20:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.expr = &BinaryExpr{Left: yyDollar[1].expr, Operator: DivStr, Right: yyDollar[3].expr}
		}
	case 21:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.expr = &BinaryExpr{Left: yyDollar[1].expr, Operator: ModStr, Right: yyDollar[3].expr}
		}
	case 22:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.expr = &BinaryExpr{Left: yyDollar[1].expr, Operator: BitAndStr, Right: yyDollar[3].expr}
		}
	case 23:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.expr = &BinaryExpr{Left: yyDollar[1].expr, Operator: BitOrStr, Right: yyDollar[3].expr}
		}
	case 24:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.expr = &BinaryExpr{Left: yyDollar[1].expr, Operator: ShiftLeftStr, Right: yyDollar[3].expr}
		}
	case 25:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.expr = &BinaryExpr{Left: yyDollar[1].expr, Operator: ShiftRightStr, Right: yyDollar[3].expr}
		}
	case 26:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.expr = &BinaryExpr{Left: yyDollar[1].expr, Operator: ConcatStr, Right: yyDollar[3].expr}
		}
	case 27:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.expr = &BinaryExpr{Left: yyDollar[1].expr, Operator: JSONExtractOp, Right: yyDollar[3].expr}
		}
	case 28:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.expr = &BinaryExpr{Left: yyDollar[1].expr, Operator: JSONUnquoteExtractOp, Right: yyDollar[3].expr}
		}
	case 29:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.expr = &CmpExpr{Left: yyDollar[1].expr, Operator: yyDollar[2].string, Right: yyDollar[3].expr}
		}
	case 30:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.expr = &CmpExpr{Left: yyDollar[1].expr, Operator: yyDollar[2].string, Right: yyDollar[3].expr}
		}
	case 31:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.expr = &CmpExpr{Left: yyDollar[1].expr, Operator: yyDollar[2].string, Right: yyDollar[3].expr}
		}
	case 32:
		yyDollar = yyS[yypt-5 : yypt+1]
		{
			yyVAL.expr = &CmpExpr{Left: yyDollar[1].expr, Operator: yyDollar[2].string, Right: yyDollar[3].expr, Escape: yyDollar[5].expr}
		}
	case 33:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			if value, ok := yyDollar[2].expr.(*Value); ok && value.Type == IntValue {
				yyVAL.expr = &Value{Type: IntValue, Value: append([]byte("-"), value.Value...)}
			} else {
				yyVAL.expr = &UnaryExpr{Operator: UMinusStr, Expr: yyDollar[2].expr}
			}
		}
	case 34:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.expr = &UnaryExpr{Operator: UPlusStr, Expr: yyDollar[2].expr}
		}
	case 35:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.expr = &UnaryExpr{Operator: TildaStr, Expr: yyDollar[2].expr}
		}
	case 36:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.expr = &AndExpr{Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 37:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.expr = &OrExpr{Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 38:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.expr = &IsExpr{Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 39:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.expr = &IsExpr{Left: yyDollar[1].expr, Right: &NotExpr{Expr: yyDollar[4].expr}}
		}
	case 40:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.expr = &IsNullExpr{Expr: yyDollar[1].expr}
		}
	case 41:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.expr = &NotNullExpr{Expr: yyDollar[1].expr}
		}
	case 42:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.expr = &NotNullExpr{Expr: yyDollar[1].expr}
		}
	case 43:
		yyDollar = yyS[yypt-5 : yypt+1]
		{
			yyVAL.expr = &BetweenExpr{Left: yyDollar[1].expr, Operator: yyDollar[2].string, From: yyDollar[3].expr, To: yyDollar[5].expr}
		}
	case 44:
		yyDollar = yyS[yypt-5 : yypt+1]
		{
			yyVAL.expr = &CaseExpr{Expr: yyDollar[2].expr, Whens: yyDollar[3].whens, Else: yyDollar[4].expr}
		}
	case 45:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.expr = &CollateExpr{Expr: yyDollar[1].expr, CollationName: string(yyDollar[3].bytes)}
		}
	case 46:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.expr = yyDollar[2].exprs
		}
	case 48:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.expr = &Value{Type: StrValue, Value: yyDollar[1].bytes[1 : len(yyDollar[1].bytes)-1]}
		}
	case 49:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.expr = &Value{Type: IntValue, Value: yyDollar[1].bytes}
		}
	case 50:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.expr = &Value{Type: FloatValue, Value: yyDollar[1].bytes}
		}
	case 51:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.expr = &Value{Type: BlobValue, Value: yyDollar[1].bytes}
		}
	case 52:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.expr = &Value{Type: HexNumValue, Value: yyDollar[1].bytes}
		}
	case 53:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.expr = BoolValue(true)
		}
	case 54:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.expr = BoolValue(false)
		}
	case 55:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.expr = &NullValue{}
		}
	case 56:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			if len(yyDollar[1].bytes) > MaxColumnNameLength {
				yylex.Error(__yyfmt__.Sprintf("column length greater than %d", MaxColumnNameLength))
				return 1
			}
			yyVAL.column = &Column{Name: string(yyDollar[1].bytes)}
		}
	case 57:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.string = EqualStr
		}
	case 58:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.string = NotEqualStr
		}
	case 59:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.string = RegexpStr
		}
	case 60:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.string = NotRegexpStr
		}
	case 61:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.string = GlobStr
		}
	case 62:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.string = NotGlobStr
		}
	case 63:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.string = MatchStr
		}
	case 64:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.string = NotMatchStr
		}
	case 65:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.string = LessThanStr
		}
	case 66:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.string = GreaterThanStr
		}
	case 67:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.string = LessEqualStr
		}
	case 68:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.string = GreaterEqualStr
		}
	case 69:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.string = LikeStr
		}
	case 70:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.string = NotLikeStr
		}
	case 71:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.string = BetweenStr
		}
	case 72:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.string = NotBetweenStr
		}
	case 73:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.convertType = NoneStr
		}
	case 74:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.convertType = TextStr
		}
	case 75:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.convertType = RealStr
		}
	case 76:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.convertType = IntegerStr
		}
	case 77:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.convertType = NumericStr
		}
	case 78:
		yyDollar = yyS[yypt-6 : yypt+1]
		{
			yyVAL.expr = &ConvertExpr{Expr: yyDollar[3].expr, Type: yyDollar[5].convertType}
		}
	case 79:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.exprs = Exprs{yyDollar[1].expr}
		}
	case 80:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.exprs = append(yyDollar[1].exprs, yyDollar[3].expr)
		}
	case 81:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.expr = nil
		}
	case 82:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.expr = yyDollar[1].exprs
		}
	case 83:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.when = &When{Condition: yyDollar[2].expr, Value: yyDollar[4].expr}
		}
	case 84:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.whens = []*When{yyDollar[1].when}
		}
	case 85:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.whens = append(yyDollar[1].whens, yyDollar[2].when)
		}
	case 86:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.expr = nil
		}
	case 87:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.expr = yyDollar[2].expr
		}
	}
	goto yystack /* stack new state and value */
}
