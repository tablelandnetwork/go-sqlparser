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
			expr:     "SELECT true FROM t",
			deparsed: "select true from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: BoolValue(true),
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "bool-value-true-upper",
			expr:     "SELECT TRUE FROM t",
			deparsed: "select true from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: BoolValue(true),
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "bool-value-false",
			expr:     "SELECT false FROM t",
			deparsed: "select false from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: BoolValue(false),
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "bool-value-false-upper",
			expr:     "SELECT FALSE FROM t",
			deparsed: "select false from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: BoolValue(false),
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "string",
			expr:     "SELECT 'anything betwen single quotes is a string' FROM t",
			deparsed: "select 'anything betwen single quotes is a string' from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &Value{
								Type:  StrValue,
								Value: []byte("anything betwen single quotes is a string")},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "string-escape",
			expr:     "SELECT 'bruno''s car' FROM t",
			deparsed: "select 'bruno''s car' from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &Value{Type: StrValue, Value: []byte("bruno''s car")},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "integer",
			expr:     "SELECT 12 FROM t",
			deparsed: "select 12 from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &Value{Type: IntValue, Value: []byte("12")},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "integer-negative",
			expr:     "SELECT -12 FROM t",
			deparsed: "select -12 from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &Value{Type: IntValue, Value: []byte("-12")},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "float",
			expr:     "SELECT 1.2 FROM t",
			deparsed: "select 1.2 from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &Value{Type: FloatValue, Value: []byte("1.2")},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "float-starts-zero",
			expr:     "SELECT 0.2 FROM t",
			deparsed: "select 0.2 from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &Value{Type: FloatValue, Value: []byte("0.2")},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "float-starts-dot",
			expr:     "SELECT .2 FROM t",
			deparsed: "select .2 from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &Value{Type: FloatValue, Value: []byte(".2")},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "float-expoent",
			expr:     "SELECT 1e2 FROM t",
			deparsed: "select 1e2 from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &Value{Type: FloatValue, Value: []byte("1e2")},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "float-expoent-upper",
			expr:     "SELECT 1E2 FROM t",
			deparsed: "select 1E2 from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &Value{Type: FloatValue, Value: []byte("1E2")},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "hex",
			expr:     "SELECT 0xAF12 FROM t",
			deparsed: "select 0xAF12 from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &Value{Type: HexNumValue, Value: []byte("0xAF12")},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "blob",
			expr:     "SELECT x'AF12' FROM t",
			deparsed: "select X'AF12' from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &Value{Type: BlobValue, Value: []byte("AF12")},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "blob-upper",
			expr:     "SELECT X'AF12' FROM t",
			deparsed: "select X'AF12' from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &Value{Type: BlobValue, Value: []byte("AF12")},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "null",
			expr:     "SELECT null FROM t",
			deparsed: "select null from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &NullValue{},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "null-upper",
			expr:     "SELECT NULL FROM t",
			deparsed: "select null from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &NullValue{},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(tc testCase) func(t *testing.T) {
			return func(t *testing.T) {
				t.Parallel()
				ast, err := Parse(tc.expr)

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
			expr:     "SELECT thisisacolumn FROM t",
			deparsed: "select thisisacolumn from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &Column{Name: "thisisacolumn"},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "column-numbers-underscore",
			expr:     "SELECT this_is_a_column3208ADKJHKDS_ FROM t",
			deparsed: "select this_is_a_column3208ADKJHKDS_ from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &Column{Name: "this_is_a_column3208ADKJHKDS_"},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "column-starts-with-underscore",
			expr:     "SELECT _also_column FROM t",
			deparsed: "select _also_column from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &Column{Name: "_also_column"},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:        "column-max-size",
			expr:        "SELECT aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			deparsed:    "",
			expectedAST: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(tc testCase) func(t *testing.T) {
			return func(t *testing.T) {
				t.Parallel()

				ast, err := Parse(tc.expr)
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
			expr:     "SELECT -2.3 FROM t",
			deparsed: "select -2.3 from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &UnaryExpr{
								Operator: UMinusStr,
								Expr:     &Value{Type: FloatValue, Value: []byte("2.3")},
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "minus-column",
			expr:     "SELECT -column FROM t",
			deparsed: "select -column from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &UnaryExpr{Operator: UMinusStr, Expr: &Column{Name: "column"}},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "double-unary-column",
			expr:     "SELECT - -column FROM t",
			deparsed: "select - -column from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &UnaryExpr{
								Operator: UMinusStr,
								Expr: &UnaryExpr{
									Operator: UMinusStr,
									Expr:     &Column{Name: "column"}},
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "comparison-equals",
			expr:     "SELECT a = 2 FROM t",
			deparsed: "select a = 2 from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &CmpExpr{
								Operator: EqualStr,
								Left:     &Column{Name: "a"},
								Right:    &Value{Type: IntValue, Value: []byte("2")},
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "comparison-not-equals",
			expr:     "SELECT a != 2 FROM t",
			deparsed: "select a != 2 from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &CmpExpr{
								Operator: NotEqualStr,
								Left:     &Column{Name: "a"},
								Right:    &Value{Type: IntValue, Value: []byte("2")},
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "comparison-not-equals-<>",
			expr:     "SELECT a <> 2 FROM t",
			deparsed: "select a != 2 from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &CmpExpr{
								Operator: NotEqualStr,
								Left:     &Column{Name: "a"},
								Right:    &Value{Type: IntValue, Value: []byte("2")},
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "comparison-greater",
			expr:     "SELECT a > 2 FROM t",
			deparsed: "select a > 2 from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &CmpExpr{
								Operator: GreaterThanStr,
								Left:     &Column{Name: "a"},
								Right:    &Value{Type: IntValue, Value: []byte("2")},
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "comparison-less",
			expr:     "SELECT a < 2 FROM t",
			deparsed: "select a < 2 from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &CmpExpr{
								Operator: LessThanStr,
								Left:     &Column{Name: "a"},
								Right:    &Value{Type: IntValue, Value: []byte("2")},
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "comparison-greater-equal",
			expr:     "SELECT a >= 2 FROM t",
			deparsed: "select a >= 2 from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &CmpExpr{
								Operator: GreaterEqualStr,
								Left:     &Column{Name: "a"},
								Right:    &Value{Type: IntValue, Value: []byte("2")},
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "comparison-less-equal",
			expr:     "SELECT a <= 2 FROM t",
			deparsed: "select a <= 2 from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &CmpExpr{
								Operator: LessEqualStr,
								Left:     &Column{Name: "a"},
								Right:    &Value{Type: IntValue, Value: []byte("2")},
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "comparison-regexp",
			expr:     "SELECT a regexp 'a' FROM t",
			deparsed: "select a regexp 'a' from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &CmpExpr{
								Operator: RegexpStr,
								Left:     &Column{Name: "a"},
								Right:    &Value{Type: StrValue, Value: []byte("a")},
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "comparison-not-regexp",
			expr:     "SELECT a not regexp 'a' FROM t",
			deparsed: "select a not regexp 'a' from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &CmpExpr{
								Operator: NotRegexpStr,
								Left:     &Column{Name: "a"},
								Right:    &Value{Type: StrValue, Value: []byte("a")},
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "comparison-glob",
			expr:     "SELECT a glob 'a' FROM t",
			deparsed: "select a glob 'a' from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &CmpExpr{
								Operator: GlobStr,
								Left:     &Column{Name: "a"},
								Right:    &Value{Type: StrValue, Value: []byte("a")},
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "comparison-not-glob",
			expr:     "SELECT a not glob 'a' FROM t",
			deparsed: "select a not glob 'a' from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &CmpExpr{
								Operator: NotGlobStr,
								Left:     &Column{Name: "a"},
								Right:    &Value{Type: StrValue, Value: []byte("a")},
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "comparison-match",
			expr:     "SELECT a match 'a' FROM t",
			deparsed: "select a match 'a' from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &CmpExpr{
								Operator: MatchStr,
								Left:     &Column{Name: "a"},
								Right:    &Value{Type: StrValue, Value: []byte("a")},
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "comparison-not-match",
			expr:     "SELECT a not match 'a' FROM t",
			deparsed: "select a not match 'a' from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &CmpExpr{
								Operator: NotMatchStr,
								Left:     &Column{Name: "a"},
								Right:    &Value{Type: StrValue, Value: []byte("a")},
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "comparison-like",
			expr:     "SELECT a like 'a' FROM t",
			deparsed: "select a like 'a' from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &CmpExpr{
								Operator: LikeStr,
								Left:     &Column{Name: "a"},
								Right:    &Value{Type: StrValue, Value: []byte("a")},
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "comparison-not-like",
			expr:     "SELECT a not like 'a' FROM t",
			deparsed: "select a not like 'a' from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &CmpExpr{
								Operator: NotLikeStr,
								Left:     &Column{Name: "a"},
								Right:    &Value{Type: StrValue, Value: []byte("a")},
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "comparison-like-escape",
			expr:     "SELECT a like '%a\\%%' escape '\\' FROM t",
			deparsed: "select a like '%a\\%%' escape '\\' from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &CmpExpr{
								Operator: LikeStr,
								Left:     &Column{Name: "a"},
								Right:    &Value{Type: StrValue, Value: []byte("%a\\%%")},
								Escape:   &Value{Type: StrValue, Value: []byte("\\")},
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "comparison-not-like-escape",
			expr:     "SELECT a not like '%a\\%%' escape '\\' FROM t",
			deparsed: "select a not like '%a\\%%' escape '\\' from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &CmpExpr{
								Operator: NotLikeStr,
								Left:     &Column{Name: "a"},
								Right:    &Value{Type: StrValue, Value: []byte("%a\\%%")},
								Escape:   &Value{Type: StrValue, Value: []byte("\\")},
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "logical-and",
			expr:     "SELECT a and b FROM t",
			deparsed: "select a and b from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &AndExpr{
								Left:  &Column{Name: "a"},
								Right: &Column{Name: "b"},
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "logical-or",
			expr:     "SELECT a or b FROM t",
			deparsed: "select a or b from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &OrExpr{
								Left:  &Column{Name: "a"},
								Right: &Column{Name: "b"},
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "is",
			expr:     "SELECT a is b FROM t",
			deparsed: "select a is b from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &IsExpr{
								Left:  &Column{Name: "a"},
								Right: &Column{Name: "b"},
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "is-not",
			expr:     "SELECT a is not b FROM t",
			deparsed: "select a is not b from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &IsExpr{
								Left: &Column{Name: "a"},
								Right: &NotExpr{
									&Column{Name: "b"},
								},
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "isnull",
			expr:     "SELECT a isnull FROM t",
			deparsed: "select a isnull from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &IsNullExpr{
								Expr: &Column{Name: "a"},
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "notnull",
			expr:     "SELECT a notnull FROM t",
			deparsed: "select a notnull from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &NotNullExpr{
								Expr: &Column{Name: "a"},
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "not-null",
			expr:     "SELECT a not null FROM t",
			deparsed: "select a notnull from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &NotNullExpr{
								Expr: &Column{Name: "a"},
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "cast-to-text",
			expr:     "SELECT CAST (1 AS TEXT) FROM t",
			deparsed: "select cast (1 as text) from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &ConvertExpr{
								Expr: &Value{
									Type:  IntValue,
									Value: []byte{'1'},
								},
								Type: TextStr,
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "cast-to-real",
			expr:     "SELECT CAST (column AS REAL) FROM t",
			deparsed: "select cast (column as real) from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &ConvertExpr{
								Expr: &Column{
									Name: "column",
								},
								Type: RealStr,
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "cast-to-none",
			expr:     "SELECT CAST (column AS none) FROM t",
			deparsed: "select cast (column as none) from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &ConvertExpr{
								Expr: &Column{
									Name: "column",
								},
								Type: NoneStr,
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "cast-to-numeric",
			expr:     "SELECT CAST (column AS numeric) FROM t",
			deparsed: "select cast (column as numeric) from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &ConvertExpr{
								Expr: &Column{
									Name: "column",
								},
								Type: NumericStr,
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "cast-to-integer",
			expr:     "SELECT CAST (column AS integer) FROM t",
			deparsed: "select cast (column as integer) from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &ConvertExpr{
								Expr: &Column{
									Name: "column",
								},
								Type: IntegerStr,
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "collate",
			expr:     "SELECT c1 = c2 COLLATE rtrim FROM t",
			deparsed: "select c1 = c2 collate rtrim from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &CmpExpr{
								Operator: EqualStr,
								Left:     &Column{Name: "c1"},
								Right: &CollateExpr{
									Expr:          &Column{Name: "c2"},
									CollationName: "rtrim",
								},
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "plus",
			expr:     "SELECT c1 + 10 FROM t",
			deparsed: "select c1 + 10 from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &BinaryExpr{
								Operator: PlusStr,
								Left:     &Column{Name: "c1"},
								Right:    &Value{Type: IntValue, Value: []byte("10")},
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "minus",
			expr:     "SELECT c1 - 10 FROM t",
			deparsed: "select c1 - 10 from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &BinaryExpr{
								Operator: MinusStr,
								Left:     &Column{Name: "c1"},
								Right:    &Value{Type: IntValue, Value: []byte("10")},
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "multiplication",
			expr:     "SELECT c1 * 10 FROM t",
			deparsed: "select c1 * 10 from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &BinaryExpr{
								Operator: MultStr,
								Left:     &Column{Name: "c1"},
								Right:    &Value{Type: IntValue, Value: []byte("10")},
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "division",
			expr:     "SELECT c1 / 10 FROM t",
			deparsed: "select c1 / 10 from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &BinaryExpr{
								Operator: DivStr,
								Left:     &Column{Name: "c1"},
								Right:    &Value{Type: IntValue, Value: []byte("10")},
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "mod",
			expr:     "SELECT c1 % 10 FROM t",
			deparsed: "select c1 % 10 from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &BinaryExpr{
								Operator: ModStr,
								Left:     &Column{Name: "c1"},
								Right:    &Value{Type: IntValue, Value: []byte("10")},
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "bitand",
			expr:     "SELECT c1 & 10 FROM t",
			deparsed: "select c1 & 10 from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &BinaryExpr{
								Operator: BitAndStr,
								Left:     &Column{Name: "c1"},
								Right:    &Value{Type: IntValue, Value: []byte("10")},
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "bitor",
			expr:     "SELECT c1 | 10 FROM t",
			deparsed: "select c1 | 10 from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &BinaryExpr{
								Operator: BitOrStr,
								Left:     &Column{Name: "c1"},
								Right:    &Value{Type: IntValue, Value: []byte("10")},
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "leftshift",
			expr:     "SELECT c1 << 10 FROM t",
			deparsed: "select c1 << 10 from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &BinaryExpr{
								Operator: ShiftLeftStr,
								Left:     &Column{Name: "c1"},
								Right:    &Value{Type: IntValue, Value: []byte("10")},
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "rightshift",
			expr:     "SELECT c1 >> 10 FROM t",
			deparsed: "select c1 >> 10 from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &BinaryExpr{
								Operator: ShiftRightStr,
								Left:     &Column{Name: "c1"},
								Right:    &Value{Type: IntValue, Value: []byte("10")},
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "concat",
			expr:     "SELECT c1 || c2 FROM t",
			deparsed: "select c1 || c2 from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &BinaryExpr{
								Operator: ConcatStr,
								Left:     &Column{Name: "c1"},
								Right:    &Column{Name: "c2"},
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "json-extract",
			expr:     "SELECT c1 -> c2 FROM t",
			deparsed: "select c1 -> c2 from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &BinaryExpr{
								Operator: JSONExtractOp,
								Left:     &Column{Name: "c1"},
								Right:    &Column{Name: "c2"},
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "json-unquote-extract",
			expr:     "SELECT c1 ->> c2 FROM t",
			deparsed: "select c1 ->> c2 from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &BinaryExpr{
								Operator: JSONUnquoteExtractOp,
								Left:     &Column{Name: "c1"},
								Right:    &Column{Name: "c2"},
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "bitnot",
			expr:     "SELECT ~c FROM t",
			deparsed: "select ~c from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &UnaryExpr{
								Operator: TildaStr,
								Expr:     &Column{Name: "c"},
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "unary-plus",
			expr:     "SELECT +c FROM t",
			deparsed: "select +c from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &UnaryExpr{
								Operator: UPlusStr,
								Expr:     &Column{Name: "c"},
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "between",
			expr:     "SELECT c1 BETWEEN c2 AND c3 FROM t",
			deparsed: "select c1 between c2 and c3 from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &BetweenExpr{
								Operator: BetweenStr,
								Left:     &Column{Name: "c1"},
								From:     &Column{Name: "c2"},
								To:       &Column{Name: "c3"},
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "between-not",
			expr:     "SELECT c1 NOT BETWEEN c2 AND c3 FROM t",
			deparsed: "select c1 not between c2 and c3 from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &BetweenExpr{
								Operator: NotBetweenStr,
								Left:     &Column{Name: "c1"},
								From:     &Column{Name: "c2"},
								To:       &Column{Name: "c3"},
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "expression-list",
			expr:     "SELECT (c1, c2, 1) FROM t",
			deparsed: "select (c1, c2, 1) from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: Exprs{
								&Column{Name: "c1"},
								&Column{Name: "c2"},
								&Value{Type: IntValue, Value: []byte("1")},
							},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "case",
			expr:     "SELECT CASE c1 WHEN 0 THEN 'zero' WHEN 1 THEN 'one' ELSE 'panic' END FROM t",
			deparsed: "select case c1 when 0 then 'zero' when 1 then 'one' else 'panic' end from t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: []SelectColumn{
						&AliasedSelectColumn{
							Expr: &CaseExpr{
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
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "t"},
						},
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(tc testCase) func(t *testing.T) {
			return func(t *testing.T) {
				t.Parallel()
				ast, err := Parse(tc.expr)
				require.NoError(t, err)
				require.Equal(t, tc.expectedAST, ast)
				require.Equal(t, tc.deparsed, ast.ToString())
			}
		}(tc))
	}
}

func TestSelectStatement(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name        string
		stmt        string
		deparsed    string
		expectedAST *AST
	}

	tests := []testCase{
		{
			name:     "simple-select",
			stmt:     "SELECT * FROM table WHERE c1 > c2",
			deparsed: "select * from table where c1 > c2",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: SelectColumnList{
						&StarSelectColumn{},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "table"},
						},
					},
					Where: &Where{
						Type: WhereStr,
						Expr: &CmpExpr{
							Operator: GreaterThanStr,
							Left:     &Column{Name: "c1"},
							Right:    &Column{Name: "c2"},
						},
					},
				},
			},
		},
		{
			name:     "multiple-columns",
			stmt:     "SELECT a, table.b, column as c1, column2 as c2, * FROM table WHERE 1",
			deparsed: "select a, table.b, column as c1, column2 as c2, * from table where 1",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: SelectColumnList{
						&AliasedSelectColumn{Expr: &Column{Name: "a"}},
						&AliasedSelectColumn{Expr: &Column{Name: "b", TableRef: &Table{Name: "table"}}},
						&AliasedSelectColumn{Expr: &Column{Name: "column"}, As: &Column{Name: "c1"}},
						&AliasedSelectColumn{Expr: &Column{Name: "column2"}, As: &Column{Name: "c2"}},
						&StarSelectColumn{},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "table"},
						},
					},
					Where: &Where{
						Type: WhereStr,
						Expr: &Value{Type: IntValue, Value: []byte("1")},
					},
				},
			},
		},
		{
			name:     "groupby",
			stmt:     "SELECT a, b FROM table GROUP BY a, b",
			deparsed: "select a, b from table group by a, b",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: SelectColumnList{
						&AliasedSelectColumn{
							Expr: &Column{Name: "a"},
						},
						&AliasedSelectColumn{
							Expr: &Column{Name: "b"},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "table"},
						},
					},
					GroupBy: []Expr{
						&Column{Name: "a"},
						&Column{Name: "b"},
					},
				},
			},
		},
		{
			name:     "groupby-having",
			stmt:     "SELECT a, b FROM table GROUP BY a, b HAVING a = 1",
			deparsed: "select a, b from table group by a, b having a = 1",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: SelectColumnList{
						&AliasedSelectColumn{
							Expr: &Column{Name: "a"},
						},
						&AliasedSelectColumn{
							Expr: &Column{Name: "b"},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "table"},
						},
					},
					GroupBy: []Expr{
						&Column{Name: "a"},
						&Column{Name: "b"},
					},
					Having: &Where{
						Type: HavingStr,
						Expr: &CmpExpr{
							Operator: EqualStr,
							Left:     &Column{Name: "a"},
							Right:    &Value{Type: IntValue, Value: []byte("1")},
						},
					},
				},
			},
		},
		{
			name:     "orderby",
			stmt:     "SELECT a, b FROM table ORDER BY a",
			deparsed: "select a, b from table order by a asc",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: SelectColumnList{
						&AliasedSelectColumn{
							Expr: &Column{Name: "a"},
						},
						&AliasedSelectColumn{
							Expr: &Column{Name: "b"},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "table"},
						},
					},
					OrderBy: OrderBy{
						&OrderingTerm{Expr: &Column{Name: "a"}, Direction: AscStr},
					},
				},
			},
		},
		{
			name:     "orderby-asc",
			stmt:     "SELECT a, b FROM table ORDER BY a asc",
			deparsed: "select a, b from table order by a asc",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: SelectColumnList{
						&AliasedSelectColumn{
							Expr: &Column{Name: "a"},
						},
						&AliasedSelectColumn{
							Expr: &Column{Name: "b"},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "table"},
						},
					},
					OrderBy: OrderBy{
						&OrderingTerm{Expr: &Column{Name: "a"}, Direction: AscStr},
					},
				},
			},
		},
		{
			name:     "orderby-asc",
			stmt:     "SELECT a, b FROM table ORDER BY a desc",
			deparsed: "select a, b from table order by a desc",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: SelectColumnList{
						&AliasedSelectColumn{
							Expr: &Column{Name: "a"},
						},
						&AliasedSelectColumn{
							Expr: &Column{Name: "b"},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "table"},
						},
					},
					OrderBy: OrderBy{
						&OrderingTerm{Expr: &Column{Name: "a"}, Direction: DescStr},
					},
				},
			},
		},
		{
			name:     "orderby-desc-asc",
			stmt:     "SELECT a, b FROM table ORDER BY a desc, b",
			deparsed: "select a, b from table order by a desc, b asc",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: SelectColumnList{
						&AliasedSelectColumn{
							Expr: &Column{Name: "a"},
						},
						&AliasedSelectColumn{
							Expr: &Column{Name: "b"},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "table"},
						},
					},
					OrderBy: OrderBy{
						&OrderingTerm{Expr: &Column{Name: "a"}, Direction: DescStr},
						&OrderingTerm{Expr: &Column{Name: "b"}, Direction: AscStr},
					},
				},
			},
		},
		{
			name:     "orderby-nulls",
			stmt:     "SELECT a, b, c FROM table ORDER BY a desc, b NULLS FIRST, c NULLS LAST",
			deparsed: "select a, b, c from table order by a desc, b asc nulls first, c asc nulls last",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: SelectColumnList{
						&AliasedSelectColumn{
							Expr: &Column{Name: "a"},
						},
						&AliasedSelectColumn{
							Expr: &Column{Name: "b"},
						},
						&AliasedSelectColumn{
							Expr: &Column{Name: "c"},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "table"},
						},
					},
					OrderBy: OrderBy{
						&OrderingTerm{Expr: &Column{Name: "a"}, Direction: DescStr, Nulls: NullsNil},
						&OrderingTerm{Expr: &Column{Name: "b"}, Direction: AscStr, Nulls: NullsFirst},
						&OrderingTerm{Expr: &Column{Name: "c"}, Direction: AscStr, Nulls: NullsLast},
					},
				},
			},
		},
		{
			name:     "limit",
			stmt:     "SELECT * FROM table LIMIT 1",
			deparsed: "select * from table limit 1",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: SelectColumnList{
						&StarSelectColumn{},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "table"},
						},
					},
					Limit: &Limit{
						Limit: &Value{Type: IntValue, Value: []byte("1")},
					},
				},
			},
		},
		{
			name:     "limit-offet",
			stmt:     "SELECT * FROM table LIMIT 1 OFFSET 2",
			deparsed: "select * from table limit 1 offset 2",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: SelectColumnList{
						&StarSelectColumn{},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "table"},
						},
					},
					Limit: &Limit{
						Limit:  &Value{Type: IntValue, Value: []byte("1")},
						Offset: &Value{Type: IntValue, Value: []byte("2")},
					},
				},
			},
		},
		{
			name:     "limit-offet-alternative",
			stmt:     "SELECT * FROM table LIMIT 1, 2",
			deparsed: "select * from table limit 2 offset 1",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: SelectColumnList{
						&StarSelectColumn{},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "table"},
						},
					},
					Limit: &Limit{
						Limit:  &Value{Type: IntValue, Value: []byte("2")},
						Offset: &Value{Type: IntValue, Value: []byte("1")},
					},
				},
			},
		},
		{
			name:     "simple-select-distinct",
			stmt:     "SELECT DISTINCT * FROM table",
			deparsed: "select distinct * from table",
			expectedAST: &AST{
				Root: &Select{
					Distinct: DistinctStr,
					SelectColumnList: SelectColumnList{
						&StarSelectColumn{},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "table"},
						},
					},
				},
			},
		},
		{
			name:     "simple-select-all",
			stmt:     "SELECT ALL * FROM table",
			deparsed: "select all * from table",
			expectedAST: &AST{
				Root: &Select{
					Distinct: AllStr,
					SelectColumnList: SelectColumnList{
						&StarSelectColumn{},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "table"},
						},
					},
				},
			},
		},
		{
			name:     "simple-select-alias-table",
			stmt:     "SELECT * FROM table as t",
			deparsed: "select * from table as t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: SelectColumnList{
						&StarSelectColumn{},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "table"},
							As:   &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "simple-select-alias-table-alt",
			stmt:     "SELECT * FROM table t",
			deparsed: "select * from table as t",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: SelectColumnList{
						&StarSelectColumn{},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "table"},
							As:   &Table{Name: "t"},
						},
					},
				},
			},
		},
		{
			name:     "select-multiple-tables",
			stmt:     "SELECT a.*, b.column1 as c1 FROM a, b",
			deparsed: "select a.*, b.column1 as c1 from a, b",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: SelectColumnList{
						&StarSelectColumn{TableRef: &Table{Name: "a"}},
						&AliasedSelectColumn{
							Expr: &Column{Name: "column1", TableRef: &Table{Name: "b"}},
							As:   &Column{Name: "c1"},
						},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Table{Name: "a"},
						},
						&AliasedTableExpr{
							Expr: &Table{Name: "b"},
						},
					},
				},
			},
		},
		{
			name:     "select-from-subquery",
			stmt:     "SELECT * FROM (SELECT * FROM table)",
			deparsed: "select * from (select * from table)",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: SelectColumnList{
						&StarSelectColumn{},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Subquery{
								Select: &Select{
									SelectColumnList: SelectColumnList{
										&StarSelectColumn{},
									},
									From: TableExprList{
										&AliasedTableExpr{
											Expr: &Table{Name: "table"},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name:     "select-from-subquery-aliased",
			stmt:     "SELECT * FROM (SELECT * FROM table) as subquery",
			deparsed: "select * from (select * from table) as subquery",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: SelectColumnList{
						&StarSelectColumn{},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Subquery{
								Select: &Select{
									SelectColumnList: SelectColumnList{
										&StarSelectColumn{},
									},
									From: TableExprList{
										&AliasedTableExpr{
											Expr: &Table{Name: "table"},
										},
									},
								},
							},
							As: &Table{Name: "subquery"},
						},
					},
				},
			},
		},
		{
			name:     "select-from-subquery-aliased-alt",
			stmt:     "SELECT * FROM (SELECT * FROM table) subquery",
			deparsed: "select * from (select * from table) as subquery",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: SelectColumnList{
						&StarSelectColumn{},
					},
					From: TableExprList{
						&AliasedTableExpr{
							Expr: &Subquery{
								Select: &Select{
									SelectColumnList: SelectColumnList{
										&StarSelectColumn{},
									},
									From: TableExprList{
										&AliasedTableExpr{
											Expr: &Table{Name: "table"},
										},
									},
								},
							},
							As: &Table{Name: "subquery"},
						},
					},
				},
			},
		},
		{
			name:     "join",
			stmt:     "SELECT * FROM a JOIN b JOIN c JOIN d",
			deparsed: "select * from a join b join c join d",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: SelectColumnList{
						&StarSelectColumn{},
					},
					From: TableExprList{
						&JoinTableExpr{
							LeftExpr: &JoinTableExpr{
								LeftExpr: &JoinTableExpr{
									LeftExpr:     &AliasedTableExpr{Expr: &Table{Name: "a"}},
									JoinOperator: JoinStr,
									RightExpr:    &AliasedTableExpr{Expr: &Table{Name: "b"}},
								},
								JoinOperator: JoinStr,
								RightExpr:    &AliasedTableExpr{Expr: &Table{Name: "c"}},
							},
							JoinOperator: JoinStr,
							RightExpr:    &AliasedTableExpr{Expr: &Table{Name: "d"}},
						},
					},
				},
			},
		},
		{
			name:     "join-on",
			stmt:     "SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.c1 = c.c1",
			deparsed: "select * from a join b on a.id = b.id join c on b.c1 = c.c1",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: SelectColumnList{
						&StarSelectColumn{},
					},
					From: TableExprList{
						&JoinTableExpr{
							LeftExpr: &JoinTableExpr{
								LeftExpr:     &AliasedTableExpr{Expr: &Table{Name: "a"}},
								JoinOperator: JoinStr,
								RightExpr:    &AliasedTableExpr{Expr: &Table{Name: "b"}},
								On: &CmpExpr{
									Operator: EqualStr,
									Left:     &Column{Name: "id", TableRef: &Table{Name: "a"}},
									Right:    &Column{Name: "id", TableRef: &Table{Name: "b"}},
								},
							},
							JoinOperator: JoinStr,
							RightExpr:    &AliasedTableExpr{Expr: &Table{Name: "c"}},
							On: &CmpExpr{
								Operator: EqualStr,
								Left:     &Column{Name: "c1", TableRef: &Table{Name: "b"}},
								Right:    &Column{Name: "c1", TableRef: &Table{Name: "c"}},
							},
						},
					},
				},
			},
		},
		{
			name:     "join-using",
			stmt:     "SELECT * FROM a JOIN b USING(c1, c2)",
			deparsed: "select * from a join b using (c1, c2)",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: SelectColumnList{
						&StarSelectColumn{},
					},
					From: TableExprList{
						&JoinTableExpr{
							LeftExpr:     &AliasedTableExpr{Expr: &Table{Name: "a"}},
							JoinOperator: JoinStr,
							RightExpr:    &AliasedTableExpr{Expr: &Table{Name: "b"}},
							Using: ColumnList{
								&Column{Name: "c1"},
								&Column{Name: "c2"},
							},
						},
					},
				},
			},
		},
		{
			name:     "subquery",
			stmt:     "SELECT * FROM t1 WHERE (SELECT 1 FROM t2)",
			deparsed: "select * from t1 where (select 1 from t2)",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: SelectColumnList{
						&StarSelectColumn{},
					},
					From: TableExprList{
						&AliasedTableExpr{Expr: &Table{Name: "t1"}},
					},
					Where: &Where{
						Type: WhereStr,
						Expr: &Subquery{
							Select: &Select{
								SelectColumnList: SelectColumnList{
									&AliasedSelectColumn{
										Expr: &Value{Type: IntValue, Value: []byte("1")},
									},
								},
								From: TableExprList{
									&AliasedTableExpr{Expr: &Table{Name: "t2"}},
								},
							},
						},
					},
				},
			},
		},
		{
			name:     "exists",
			stmt:     "SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2)",
			deparsed: "select * from t1 where exists (select 1 from t2)",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: SelectColumnList{
						&StarSelectColumn{},
					},
					From: TableExprList{
						&AliasedTableExpr{Expr: &Table{Name: "t1"}},
					},
					Where: &Where{
						Type: WhereStr,
						Expr: &ExistsExpr{
							&Subquery{
								Select: &Select{
									SelectColumnList: SelectColumnList{
										&AliasedSelectColumn{
											Expr: &Value{Type: IntValue, Value: []byte("1")},
										},
									},
									From: TableExprList{
										&AliasedTableExpr{Expr: &Table{Name: "t2"}},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name:     "not-exists",
			stmt:     "SELECT * FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2)",
			deparsed: "select * from t1 where not exists (select 1 from t2)",
			expectedAST: &AST{
				Root: &Select{
					SelectColumnList: SelectColumnList{
						&StarSelectColumn{},
					},
					From: TableExprList{
						&AliasedTableExpr{Expr: &Table{Name: "t1"}},
					},
					Where: &Where{
						Type: WhereStr,
						Expr: &NotExpr{
							Expr: &ExistsExpr{
								&Subquery{
									Select: &Select{
										SelectColumnList: SelectColumnList{
											&AliasedSelectColumn{
												Expr: &Value{Type: IntValue, Value: []byte("1")},
											},
										},
										From: TableExprList{
											&AliasedTableExpr{Expr: &Table{Name: "t2"}},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(tc testCase) func(t *testing.T) {
			return func(t *testing.T) {
				t.Parallel()
				ast, err := Parse(tc.stmt)
				require.NoError(t, err)
				require.Equal(t, tc.expectedAST, ast)
				require.Equal(t, tc.deparsed, ast.ToString())
			}
		}(tc))
	}
}
