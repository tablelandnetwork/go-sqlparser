package sqlparser

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValueLiteral(t *testing.T) {
	t.Parallel()
	type testCase struct {
		name        string
		expr        string
		deparsed    string
		expectedAST *AST
	}

	tests := []testCase{
		{
			name:     "bool-value-true",
			expr:     "true",
			deparsed: "true",
			expectedAST: &AST{
				Root: BoolValue(true),
			},
		},
		{
			name:     "bool-value-true-upper",
			expr:     "TRUE",
			deparsed: "true",
			expectedAST: &AST{
				Root: BoolValue(true),
			},
		},
		{
			name:     "bool-value-false",
			expr:     "false",
			deparsed: "false",
			expectedAST: &AST{
				Root: BoolValue(false),
			},
		},
		{
			name:     "bool-value-false-upper",
			expr:     "FALSE",
			deparsed: "false",
			expectedAST: &AST{
				Root: BoolValue(false),
			},
		},
		{
			name:     "string",
			expr:     "'anything betwen single quotes is a string'",
			deparsed: "'anything betwen single quotes is a string'",
			expectedAST: &AST{
				Root: &Value{Type: StrValue, Value: []byte("anything betwen single quotes is a string")},
			},
		},
		{
			name:     "string-escape",
			expr:     "'bruno''s car'",
			deparsed: "'bruno''s car'",
			expectedAST: &AST{
				Root: &Value{Type: StrValue, Value: []byte("bruno''s car")},
			},
		},
		{
			name:     "integer",
			expr:     "12",
			deparsed: "12",
			expectedAST: &AST{
				Root: &Value{Type: IntValue, Value: []byte("12")},
			},
		},
		{
			name:     "integer-negative",
			expr:     "-12",
			deparsed: "-12",
			expectedAST: &AST{
				Root: &Value{Type: IntValue, Value: []byte("-12")},
			},
		},
		{
			name:     "float",
			expr:     "1.2",
			deparsed: "1.2",
			expectedAST: &AST{
				Root: &Value{Type: FloatValue, Value: []byte("1.2")},
			},
		},
		{
			name:     "float-starts-zero",
			expr:     "0.2",
			deparsed: "0.2",
			expectedAST: &AST{
				Root: &Value{Type: FloatValue, Value: []byte("0.2")},
			},
		},
		{
			name:     "float-starts-dot",
			expr:     ".2",
			deparsed: ".2",
			expectedAST: &AST{
				Root: &Value{Type: FloatValue, Value: []byte(".2")},
			},
		},
		{
			name:     "float-expoent",
			expr:     "1e2",
			deparsed: "1e2",
			expectedAST: &AST{
				Root: &Value{Type: FloatValue, Value: []byte("1e2")},
			},
		},
		{
			name:     "float-expoent-upper",
			expr:     "1E2",
			deparsed: "1E2",
			expectedAST: &AST{
				Root: &Value{Type: FloatValue, Value: []byte("1E2")},
			},
		},
		{
			name:     "hex",
			expr:     "0xAF12",
			deparsed: "0xAF12",
			expectedAST: &AST{
				Root: &Value{Type: HexNumValue, Value: []byte("0xAF12")},
			},
		},
		{
			name:     "blob",
			expr:     "x'AF12'",
			deparsed: "X'AF12'",
			expectedAST: &AST{
				Root: &Value{Type: BlobValue, Value: []byte("AF12")},
			},
		},
		{
			name:     "blob-upper",
			expr:     "X'AF12'",
			deparsed: "X'AF12'",
			expectedAST: &AST{
				Root: &Value{Type: BlobValue, Value: []byte("AF12")},
			},
		},
		{
			name:     "null",
			expr:     "null",
			deparsed: "null",
			expectedAST: &AST{
				Root: &NullValue{},
			},
		},
		{
			name:     "null-upper",
			expr:     "NULL",
			deparsed: "null",
			expectedAST: &AST{
				Root: &NullValue{},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(tc testCase) func(t *testing.T) {
			return func(t *testing.T) {
				t.Parallel()
				parser := NewParser()

				ast, err := parser.Parse(tc.expr)

				require.NoError(t, err)
				require.Equal(t, tc.expectedAST, ast)
				require.Equal(t, tc.deparsed, ast.ToString())
			}
		}(tc))
	}
}

func TestColumnName(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name        string
		expr        string
		deparsed    string
		expectedAST *AST
	}

	tests := []testCase{
		{
			name:     "column",
			expr:     "thisisacolumn",
			deparsed: "thisisacolumn",
			expectedAST: &AST{
				Root: &Column{Name: "thisisacolumn"},
			},
		},
		{
			name:     "column-numbers-underscore",
			expr:     "this_is_a_column3208ADKJHKDS_",
			deparsed: "this_is_a_column3208ADKJHKDS_",
			expectedAST: &AST{
				Root: &Column{Name: "this_is_a_column3208ADKJHKDS_"},
			},
		},
		{
			name:     "column-starts-with-underscore",
			expr:     "_also_column",
			deparsed: "_also_column",
			expectedAST: &AST{
				Root: &Column{Name: "_also_column"},
			},
		},
		{
			name:        "column-max-size",
			expr:        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			deparsed:    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			expectedAST: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(tc testCase) func(t *testing.T) {
			return func(t *testing.T) {
				t.Parallel()
				parser := NewParser()

				ast, err := parser.Parse(tc.expr)
				if tc.expectedAST != nil {
					require.NoError(t, err)
					require.Equal(t, tc.expectedAST, ast)
					require.Equal(t, tc.deparsed, ast.ToString())
				} else {
					require.Error(t, err)
				}

			}
		}(tc))
	}
}

func TestExpr(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name        string
		expr        string
		deparsed    string
		expectedAST *AST
	}

	tests := []testCase{
		{
			name:     "minus-float",
			expr:     "-2.3",
			deparsed: "-2.3",
			expectedAST: &AST{
				Root: &UnaryExpr{
					Operator: UMinusStr,
					Expr:     &Value{Type: FloatValue, Value: []byte("2.3")}},
			},
		},
		{
			name:     "minus-column",
			expr:     "-column",
			deparsed: "-column",
			expectedAST: &AST{
				Root: &UnaryExpr{Operator: UMinusStr, Expr: &Column{Name: "column"}},
			},
		},
		{
			name:     "double-unary-column",
			expr:     "- -column",
			deparsed: "- -column",
			expectedAST: &AST{
				Root: &UnaryExpr{
					Operator: UMinusStr,
					Expr: &UnaryExpr{
						Operator: UMinusStr,
						Expr:     &Column{Name: "column"}},
				},
			},
		},
		{
			name:     "comparison-equals",
			expr:     "a = 2",
			deparsed: "a = 2",
			expectedAST: &AST{
				Root: &CmpExpr{
					Operator: EqualStr,
					Left:     &Column{Name: "a"},
					Right:    &Value{Type: IntValue, Value: []byte("2")},
				},
			},
		},
		{
			name:     "comparison-not-equals",
			expr:     "a != 2",
			deparsed: "a != 2",
			expectedAST: &AST{
				Root: &CmpExpr{
					Operator: NotEqualStr,
					Left:     &Column{Name: "a"},
					Right:    &Value{Type: IntValue, Value: []byte("2")},
				},
			},
		},
		{
			name:     "comparison-not-equals-<>",
			expr:     "a <> 2",
			deparsed: "a != 2",
			expectedAST: &AST{
				Root: &CmpExpr{
					Operator: NotEqualStr,
					Left:     &Column{Name: "a"},
					Right:    &Value{Type: IntValue, Value: []byte("2")},
				},
			},
		},
		{
			name:     "comparison-greater",
			expr:     "a > 2",
			deparsed: "a > 2",
			expectedAST: &AST{
				Root: &CmpExpr{
					Operator: GreaterThanStr,
					Left:     &Column{Name: "a"},
					Right:    &Value{Type: IntValue, Value: []byte("2")},
				},
			},
		},
		{
			name:     "comparison-less",
			expr:     "a < 2",
			deparsed: "a < 2",
			expectedAST: &AST{
				Root: &CmpExpr{
					Operator: LessThanStr,
					Left:     &Column{Name: "a"},
					Right:    &Value{Type: IntValue, Value: []byte("2")},
				},
			},
		},
		{
			name:     "comparison-greater-equal",
			expr:     "a >= 2",
			deparsed: "a >= 2",
			expectedAST: &AST{
				Root: &CmpExpr{
					Operator: GreaterEqualStr,
					Left:     &Column{Name: "a"},
					Right:    &Value{Type: IntValue, Value: []byte("2")},
				},
			},
		},
		{
			name:     "comparison-less-equal",
			expr:     "a <= 2",
			deparsed: "a <= 2",
			expectedAST: &AST{
				Root: &CmpExpr{
					Operator: LessEqualStr,
					Left:     &Column{Name: "a"},
					Right:    &Value{Type: IntValue, Value: []byte("2")},
				},
			},
		},
		{
			name:     "comparison-regexp",
			expr:     "a regexp 'a'",
			deparsed: "a regexp 'a'",
			expectedAST: &AST{
				Root: &CmpExpr{
					Operator: RegexpStr,
					Left:     &Column{Name: "a"},
					Right:    &Value{Type: StrValue, Value: []byte("a")},
				},
			},
		},
		{
			name:     "comparison-not-regexp",
			expr:     "a not regexp 'a'",
			deparsed: "a not regexp 'a'",
			expectedAST: &AST{
				Root: &CmpExpr{
					Operator: NotRegexpStr,
					Left:     &Column{Name: "a"},
					Right:    &Value{Type: StrValue, Value: []byte("a")},
				},
			},
		},
		{
			name:     "comparison-glob",
			expr:     "a glob 'a'",
			deparsed: "a glob 'a'",
			expectedAST: &AST{
				Root: &CmpExpr{
					Operator: GlobStr,
					Left:     &Column{Name: "a"},
					Right:    &Value{Type: StrValue, Value: []byte("a")},
				},
			},
		},
		{
			name:     "comparison-not-glob",
			expr:     "a not glob 'a'",
			deparsed: "a not glob 'a'",
			expectedAST: &AST{
				Root: &CmpExpr{
					Operator: NotGlobStr,
					Left:     &Column{Name: "a"},
					Right:    &Value{Type: StrValue, Value: []byte("a")},
				},
			},
		},
		{
			name:     "comparison-match",
			expr:     "a match 'a'",
			deparsed: "a match 'a'",
			expectedAST: &AST{
				Root: &CmpExpr{
					Operator: MatchStr,
					Left:     &Column{Name: "a"},
					Right:    &Value{Type: StrValue, Value: []byte("a")},
				},
			},
		},
		{
			name:     "comparison-not-match",
			expr:     "a not match 'a'",
			deparsed: "a not match 'a'",
			expectedAST: &AST{
				Root: &CmpExpr{
					Operator: NotMatchStr,
					Left:     &Column{Name: "a"},
					Right:    &Value{Type: StrValue, Value: []byte("a")},
				},
			},
		},
		{
			name:     "comparison-like",
			expr:     "a like 'a'",
			deparsed: "a like 'a'",
			expectedAST: &AST{
				Root: &CmpExpr{
					Operator: LikeStr,
					Left:     &Column{Name: "a"},
					Right:    &Value{Type: StrValue, Value: []byte("a")},
				},
			},
		},
		{
			name:     "comparison-not-like",
			expr:     "a not like 'a'",
			deparsed: "a not like 'a'",
			expectedAST: &AST{
				Root: &CmpExpr{
					Operator: NotLikeStr,
					Left:     &Column{Name: "a"},
					Right:    &Value{Type: StrValue, Value: []byte("a")},
				},
			},
		},
		{
			name:     "comparison-like-escape",
			expr:     "a like '%a\\%%' escape '\\'",
			deparsed: "a like '%a\\%%' escape '\\'",
			expectedAST: &AST{
				Root: &CmpExpr{
					Operator: LikeStr,
					Left:     &Column{Name: "a"},
					Right:    &Value{Type: StrValue, Value: []byte("%a\\%%")},
					Escape:   &Value{Type: StrValue, Value: []byte("\\")},
				},
			},
		},
		{
			name:     "comparison-not-like-escape",
			expr:     "a not like '%a\\%%' escape '\\'",
			deparsed: "a not like '%a\\%%' escape '\\'",
			expectedAST: &AST{
				Root: &CmpExpr{
					Operator: NotLikeStr,
					Left:     &Column{Name: "a"},
					Right:    &Value{Type: StrValue, Value: []byte("%a\\%%")},
					Escape:   &Value{Type: StrValue, Value: []byte("\\")},
				},
			},
		},
		{
			name:     "logical-and",
			expr:     "a and b",
			deparsed: "a and b",
			expectedAST: &AST{
				Root: &AndExpr{
					Left:  &Column{Name: "a"},
					Right: &Column{Name: "b"},
				},
			},
		},
		{
			name:     "logical-or",
			expr:     "a or b",
			deparsed: "a or b",
			expectedAST: &AST{
				Root: &OrExpr{
					Left:  &Column{Name: "a"},
					Right: &Column{Name: "b"},
				},
			},
		},
		{
			name:     "is",
			expr:     "a is b",
			deparsed: "a is b",
			expectedAST: &AST{
				Root: &IsExpr{
					Left:  &Column{Name: "a"},
					Right: &Column{Name: "b"},
				},
			},
		},
		{
			name:     "is-not",
			expr:     "a is not b",
			deparsed: "a is not b",
			expectedAST: &AST{
				Root: &IsExpr{
					Left: &Column{Name: "a"},
					Right: &NotExpr{
						&Column{Name: "b"},
					},
				},
			},
		},
		{
			name:     "isnull",
			expr:     "a isnull",
			deparsed: "a isnull",
			expectedAST: &AST{
				Root: &IsNullExpr{
					Expr: &Column{Name: "a"},
				},
			},
		},
		{
			name:     "notnull",
			expr:     "a notnull",
			deparsed: "a notnull",
			expectedAST: &AST{
				Root: &NotNullExpr{
					Expr: &Column{Name: "a"},
				},
			},
		},
		{
			name:     "not-null",
			expr:     "a not null",
			deparsed: "a notnull",
			expectedAST: &AST{
				Root: &NotNullExpr{
					Expr: &Column{Name: "a"},
				},
			},
		},
		{
			name:     "cast-to-text",
			expr:     "CAST (1 AS TEXT)",
			deparsed: "cast (1 as text)",
			expectedAST: &AST{
				Root: &ConvertExpr{
					Expr: &Value{
						Type:  IntValue,
						Value: []byte{'1'},
					},
					Type: TextStr,
				},
			},
		},
		{
			name:     "cast-to-real",
			expr:     "CAST (column AS REAL)",
			deparsed: "cast (column as real)",
			expectedAST: &AST{
				Root: &ConvertExpr{
					Expr: &Column{
						Name: "column",
					},
					Type: RealStr,
				},
			},
		},
		{
			name:     "cast-to-none",
			expr:     "CAST (column AS none)",
			deparsed: "cast (column as none)",
			expectedAST: &AST{
				Root: &ConvertExpr{
					Expr: &Column{
						Name: "column",
					},
					Type: NoneStr,
				},
			},
		},
		{
			name:     "cast-to-numeric",
			expr:     "CAST (column AS numeric)",
			deparsed: "cast (column as numeric)",
			expectedAST: &AST{
				Root: &ConvertExpr{
					Expr: &Column{
						Name: "column",
					},
					Type: NumericStr,
				},
			},
		},
		{
			name:     "cast-to-integer",
			expr:     "CAST (column AS integer)",
			deparsed: "cast (column as integer)",
			expectedAST: &AST{
				Root: &ConvertExpr{
					Expr: &Column{
						Name: "column",
					},
					Type: IntegerStr,
				},
			},
		},
		{
			name:     "collate",
			expr:     "c1 = c2 COLLATE rtrim",
			deparsed: "c1 = c2 collate rtrim",
			expectedAST: &AST{
				Root: &CmpExpr{
					Operator: EqualStr,
					Left:     &Column{Name: "c1"},
					Right: &CollateExpr{
						Expr:          &Column{Name: "c2"},
						CollationName: "rtrim",
					},
				},
			},
		},
		{
			name:     "plus",
			expr:     "c1 + 10",
			deparsed: "c1 + 10",
			expectedAST: &AST{
				Root: &BinaryExpr{
					Operator: PlusStr,
					Left:     &Column{Name: "c1"},
					Right:    &Value{Type: IntValue, Value: []byte("10")},
				},
			},
		},
		{
			name:     "minus",
			expr:     "c1 - 10",
			deparsed: "c1 - 10",
			expectedAST: &AST{
				Root: &BinaryExpr{
					Operator: MinusStr,
					Left:     &Column{Name: "c1"},
					Right:    &Value{Type: IntValue, Value: []byte("10")},
				},
			},
		},
		{
			name:     "multiplication",
			expr:     "c1 * 10",
			deparsed: "c1 * 10",
			expectedAST: &AST{
				Root: &BinaryExpr{
					Operator: MultStr,
					Left:     &Column{Name: "c1"},
					Right:    &Value{Type: IntValue, Value: []byte("10")},
				},
			},
		},
		{
			name:     "division",
			expr:     "c1 / 10",
			deparsed: "c1 / 10",
			expectedAST: &AST{
				Root: &BinaryExpr{
					Operator: DivStr,
					Left:     &Column{Name: "c1"},
					Right:    &Value{Type: IntValue, Value: []byte("10")},
				},
			},
		},
		{
			name:     "mod",
			expr:     "c1 % 10",
			deparsed: "c1 % 10",
			expectedAST: &AST{
				Root: &BinaryExpr{
					Operator: ModStr,
					Left:     &Column{Name: "c1"},
					Right:    &Value{Type: IntValue, Value: []byte("10")},
				},
			},
		},
		{
			name:     "bitand",
			expr:     "c1 & 10",
			deparsed: "c1 & 10",
			expectedAST: &AST{
				Root: &BinaryExpr{
					Operator: BitAndStr,
					Left:     &Column{Name: "c1"},
					Right:    &Value{Type: IntValue, Value: []byte("10")},
				},
			},
		},
		{
			name:     "bitor",
			expr:     "c1 | 10",
			deparsed: "c1 | 10",
			expectedAST: &AST{
				Root: &BinaryExpr{
					Operator: BitOrStr,
					Left:     &Column{Name: "c1"},
					Right:    &Value{Type: IntValue, Value: []byte("10")},
				},
			},
		},
		{
			name:     "leftshift",
			expr:     "c1 << 10",
			deparsed: "c1 << 10",
			expectedAST: &AST{
				Root: &BinaryExpr{
					Operator: ShiftLeftStr,
					Left:     &Column{Name: "c1"},
					Right:    &Value{Type: IntValue, Value: []byte("10")},
				},
			},
		},
		{
			name:     "rightshift",
			expr:     "c1 >> 10",
			deparsed: "c1 >> 10",
			expectedAST: &AST{
				Root: &BinaryExpr{
					Operator: ShiftRightStr,
					Left:     &Column{Name: "c1"},
					Right:    &Value{Type: IntValue, Value: []byte("10")},
				},
			},
		},
		{
			name:     "concat",
			expr:     "c1 || c2",
			deparsed: "c1 || c2",
			expectedAST: &AST{
				Root: &BinaryExpr{
					Operator: ConcatStr,
					Left:     &Column{Name: "c1"},
					Right:    &Column{Name: "c2"},
				},
			},
		},
		{
			name:     "json-extract",
			expr:     "c1 -> c2",
			deparsed: "c1 -> c2",
			expectedAST: &AST{
				Root: &BinaryExpr{
					Operator: JSONExtractOp,
					Left:     &Column{Name: "c1"},
					Right:    &Column{Name: "c2"},
				},
			},
		},
		{
			name:     "json-unquote-extract",
			expr:     "c1 ->> c2",
			deparsed: "c1 ->> c2",
			expectedAST: &AST{
				Root: &BinaryExpr{
					Operator: JSONUnquoteExtractOp,
					Left:     &Column{Name: "c1"},
					Right:    &Column{Name: "c2"},
				},
			},
		},
		{
			name:     "bitnot",
			expr:     "~c",
			deparsed: "~c",
			expectedAST: &AST{
				Root: &UnaryExpr{
					Operator: TildaStr,
					Expr:     &Column{Name: "c"},
				},
			},
		},
		{
			name:     "unary-plus",
			expr:     "+c",
			deparsed: "+c",
			expectedAST: &AST{
				Root: &UnaryExpr{
					Operator: UPlusStr,
					Expr:     &Column{Name: "c"},
				},
			},
		},
		{
			name:     "between",
			expr:     "c1 BETWEEN c2 AND c3",
			deparsed: "c1 between c2 and c3",
			expectedAST: &AST{
				Root: &BetweenExpr{
					Operator: BetweenStr,
					Left:     &Column{Name: "c1"},
					From:     &Column{Name: "c2"},
					To:       &Column{Name: "c3"},
				},
			},
		},
		{
			name:     "between-not",
			expr:     "c1 NOT BETWEEN c2 AND c3",
			deparsed: "c1 not between c2 and c3",
			expectedAST: &AST{
				Root: &BetweenExpr{
					Operator: NotBetweenStr,
					Left:     &Column{Name: "c1"},
					From:     &Column{Name: "c2"},
					To:       &Column{Name: "c3"},
				},
			},
		},
		{
			name:     "expression-list",
			expr:     "(c1, c2, 1)",
			deparsed: "(c1, c2, 1)",
			expectedAST: &AST{
				Root: Exprs{
					&Column{Name: "c1"},
					&Column{Name: "c2"},
					&Value{Type: IntValue, Value: []byte("1")},
				},
			},
		},
		{
			name:     "case",
			expr:     "CASE c1 WHEN 0 THEN 'zero' WHEN 1 THEN 'one' ELSE 'panic' END",
			deparsed: "case c1 when 0 then 'zero' when 1 then 'one' else 'panic' end",
			expectedAST: &AST{
				Root: &CaseExpr{
					Expr: &Column{Name: "c1"},
					Whens: []*When{
						{
							Condition: &Value{Type: IntValue, Value: []byte("0")},
							Value:     &Value{Type: StrValue, Value: []byte("zero")},
						},
						{
							Condition: &Value{Type: IntValue, Value: []byte("1")},
							Value:     &Value{Type: StrValue, Value: []byte("one")},
						},
					},
					Else: &Value{Type: StrValue, Value: []byte("panic")},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(tc testCase) func(t *testing.T) {
			return func(t *testing.T) {
				t.Parallel()
				parser := NewParser()

				ast, err := parser.Parse(tc.expr)
				require.NoError(t, err)
				require.Equal(t, tc.expectedAST, ast)
				require.Equal(t, tc.deparsed, ast.ToString())
			}
		}(tc))
	}
}

func TestPrint(t *testing.T) {
	parser := NewParser()

	ast, err := parser.Parse("c1 = 2 * c2 AND c3 IS NOT NULL")
	if err != nil {
		panic(err)
	}

	ast.PrettyPrint()
}
