package sqlparser

import (
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

func TestSelectStatement(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name        string
		stmt        string
		deparsed    string
		expectedAST *AST
		expectedErr error
	}

	tests := []testCase{
		{
			name:     "bool-value-true",
			stmt:     "SELECT true FROM t",
			deparsed: "select true from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: BoolValue(true),
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "bool-value-true-upper",
			stmt:     "SELECT TRUE FROM t",
			deparsed: "select true from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: BoolValue(true),
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "bool-value-false",
			stmt:     "SELECT false FROM t",
			deparsed: "select false from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: BoolValue(false),
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "bool-value-false-upper",
			stmt:     "SELECT FALSE FROM t",
			deparsed: "select false from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: BoolValue(false),
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "string",
			stmt:     "SELECT 'anything between single quotes is a string' FROM t",
			deparsed: "select 'anything between single quotes is a string' from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &Value{
									Type:  StrValue,
									Value: []byte("anything between single quotes is a string"),
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "string-escape",
			stmt:     "SELECT 'bruno''s car' FROM t",
			deparsed: "select 'bruno''s car' from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &Value{Type: StrValue, Value: []byte("bruno''s car")},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "integer",
			stmt:     "SELECT 12 FROM t",
			deparsed: "select 12 from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &Value{Type: IntValue, Value: []byte("12")},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "integer-negative",
			stmt:     "SELECT -12 FROM t",
			deparsed: "select -12 from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &Value{Type: IntValue, Value: []byte("-12")},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:        "float",
			stmt:        "SELECT 1.2 FROM t",
			deparsed:    "select 1.2 from t",
			expectedErr: &ErrNumericLiteralFloat{Value: []byte("1.2")},
		},
		{
			name:        "float-starts-zero",
			stmt:        "SELECT 0.2 FROM t",
			deparsed:    "select 0.2 from t",
			expectedErr: &ErrNumericLiteralFloat{Value: []byte("0.2")},
		},
		{
			name:        "float-starts-dot",
			stmt:        "SELECT .2 FROM t",
			deparsed:    "select .2 from t",
			expectedErr: &ErrNumericLiteralFloat{Value: []byte(".2")},
		},
		{
			name:        "float-expoent",
			stmt:        "SELECT 1e2 FROM t",
			deparsed:    "select 1e2 from t",
			expectedErr: &ErrNumericLiteralFloat{Value: []byte("1e2")},
		},
		{
			name:        "float-expoent-upper",
			stmt:        "SELECT 1E2 FROM t",
			deparsed:    "select 1E2 from t",
			expectedErr: &ErrNumericLiteralFloat{Value: []byte("1E2")},
		},
		{
			name:     "hex",
			stmt:     "SELECT 0xAF12 FROM t",
			deparsed: "select 0xAF12 from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &Value{Type: HexNumValue, Value: []byte("0xAF12")},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "blob",
			stmt:     "SELECT x'AF12' FROM t",
			deparsed: "select X'AF12' from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &Value{Type: BlobValue, Value: []byte("AF12")},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "blob-upper",
			stmt:     "SELECT X'AF12' FROM t",
			deparsed: "select X'AF12' from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &Value{Type: BlobValue, Value: []byte("AF12")},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "null",
			stmt:     "SELECT null FROM t",
			deparsed: "select null from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &NullValue{},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "null-upper",
			stmt:     "SELECT NULL FROM t",
			deparsed: "select null from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &NullValue{},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "column",
			stmt:     "SELECT thisisacolumn FROM t",
			deparsed: "select thisisacolumn from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &Column{Name: "thisisacolumn"},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "column-numbers-underscore",
			stmt:     "SELECT this_is_a_column3208ADKJHKDS_ FROM t",
			deparsed: "select this_is_a_column3208ADKJHKDS_ from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &Column{Name: "this_is_a_column3208ADKJHKDS_"},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "column-starts-with-underscore",
			stmt:     "SELECT _also_column FROM t",
			deparsed: "select _also_column from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &Column{Name: "_also_column"},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:        "minus-float",
			stmt:        "SELECT -2.3 FROM t",
			deparsed:    "select -2.3 from t",
			expectedErr: &ErrNumericLiteralFloat{Value: []byte("-2.3")},
		},
		{
			name:     "minus-column",
			stmt:     "SELECT -a FROM t",
			deparsed: "select -a from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &UnaryExpr{Operator: UMinusStr, Expr: &Column{Name: "a"}},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "double-unary-column",
			stmt:     "SELECT - -a FROM t",
			deparsed: "select - -a from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &UnaryExpr{
									Operator: UMinusStr,
									Expr: &UnaryExpr{
										Operator: UMinusStr,
										Expr:     &Column{Name: "a"},
									},
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "comparison-equals",
			stmt:     "SELECT a = 2 FROM t",
			deparsed: "select a=2 from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &CmpExpr{
									Operator: EqualStr,
									Left:     &Column{Name: "a"},
									Right:    &Value{Type: IntValue, Value: []byte("2")},
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "comparison-not-equals",
			stmt:     "SELECT a != 2 FROM t",
			deparsed: "select a!=2 from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &CmpExpr{
									Operator: NotEqualStr,
									Left:     &Column{Name: "a"},
									Right:    &Value{Type: IntValue, Value: []byte("2")},
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "comparison-not-equals-<>",
			stmt:     "SELECT a <> 2 FROM t",
			deparsed: "select a!=2 from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &CmpExpr{
									Operator: NotEqualStr,
									Left:     &Column{Name: "a"},
									Right:    &Value{Type: IntValue, Value: []byte("2")},
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "comparison-greater",
			stmt:     "SELECT a > 2 FROM t",
			deparsed: "select a>2 from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &CmpExpr{
									Operator: GreaterThanStr,
									Left:     &Column{Name: "a"},
									Right:    &Value{Type: IntValue, Value: []byte("2")},
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "comparison-less",
			stmt:     "SELECT a < 2 FROM t",
			deparsed: "select a<2 from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &CmpExpr{
									Operator: LessThanStr,
									Left:     &Column{Name: "a"},
									Right:    &Value{Type: IntValue, Value: []byte("2")},
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "comparison-greater-equal",
			stmt:     "SELECT a >= 2 FROM t",
			deparsed: "select a>=2 from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &CmpExpr{
									Operator: GreaterEqualStr,
									Left:     &Column{Name: "a"},
									Right:    &Value{Type: IntValue, Value: []byte("2")},
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "comparison-less-equal",
			stmt:     "SELECT a <= 2 FROM t",
			deparsed: "select a<=2 from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &CmpExpr{
									Operator: LessEqualStr,
									Left:     &Column{Name: "a"},
									Right:    &Value{Type: IntValue, Value: []byte("2")},
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "comparison-glob",
			stmt:     "SELECT a glob 'a' FROM t",
			deparsed: "select a glob 'a' from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &CmpExpr{
									Operator: GlobStr,
									Left:     &Column{Name: "a"},
									Right:    &Value{Type: StrValue, Value: []byte("a")},
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "comparison-not-glob",
			stmt:     "SELECT a not glob 'a' FROM t",
			deparsed: "select a not glob 'a' from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &CmpExpr{
									Operator: NotGlobStr,
									Left:     &Column{Name: "a"},
									Right:    &Value{Type: StrValue, Value: []byte("a")},
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "comparison-match",
			stmt:     "SELECT a match 'a' FROM t",
			deparsed: "select a match 'a' from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &CmpExpr{
									Operator: MatchStr,
									Left:     &Column{Name: "a"},
									Right:    &Value{Type: StrValue, Value: []byte("a")},
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "comparison-not-match",
			stmt:     "SELECT a not match 'a' FROM t",
			deparsed: "select a not match 'a' from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &CmpExpr{
									Operator: NotMatchStr,
									Left:     &Column{Name: "a"},
									Right:    &Value{Type: StrValue, Value: []byte("a")},
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "comparison-like",
			stmt:     "SELECT a like 'a' FROM t",
			deparsed: "select a like 'a' from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &CmpExpr{
									Operator: LikeStr,
									Left:     &Column{Name: "a"},
									Right:    &Value{Type: StrValue, Value: []byte("a")},
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "comparison-not-like",
			stmt:     "SELECT a not like 'a' FROM t",
			deparsed: "select a not like 'a' from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &CmpExpr{
									Operator: NotLikeStr,
									Left:     &Column{Name: "a"},
									Right:    &Value{Type: StrValue, Value: []byte("a")},
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "comparison-like-escape",
			stmt:     "SELECT a like '%a\\%%' escape '\\' FROM t",
			deparsed: "select a like '%a\\%%' escape '\\' from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
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
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "comparison-not-like-escape",
			stmt:     "SELECT a not like '%a\\%%' escape '\\' FROM t",
			deparsed: "select a not like '%a\\%%' escape '\\' from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
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
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "logical-and",
			stmt:     "SELECT a and b FROM t",
			deparsed: "select a and b from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &AndExpr{
									Left:  &Column{Name: "a"},
									Right: &Column{Name: "b"},
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "logical-or",
			stmt:     "SELECT a or b FROM t",
			deparsed: "select a or b from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &OrExpr{
									Left:  &Column{Name: "a"},
									Right: &Column{Name: "b"},
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "is",
			stmt:     "SELECT a is b FROM t",
			deparsed: "select a is b from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &IsExpr{
									Left:  &Column{Name: "a"},
									Right: &Column{Name: "b"},
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "is-not",
			stmt:     "SELECT a is not b FROM t",
			deparsed: "select a is not b from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
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
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "isnull",
			stmt:     "SELECT a isnull FROM t",
			deparsed: "select a isnull from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &IsNullExpr{
									Expr: &Column{Name: "a"},
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "notnull",
			stmt:     "SELECT a notnull FROM t",
			deparsed: "select a notnull from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &NotNullExpr{
									Expr: &Column{Name: "a"},
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "not-null",
			stmt:     "SELECT a not null FROM t",
			deparsed: "select a notnull from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &NotNullExpr{
									Expr: &Column{Name: "a"},
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "cast-to-text",
			stmt:     "SELECT CAST (1 AS TEXT) FROM t",
			deparsed: "select cast(1 as text)from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
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
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "cast-to-none",
			stmt:     "SELECT CAST (a AS none) FROM t",
			deparsed: "select cast(a as none)from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &ConvertExpr{
									Expr: &Column{
										Name: "a",
									},
									Type: NoneStr,
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "cast-to-integer",
			stmt:     "SELECT CAST (a AS integer) FROM t",
			deparsed: "select cast(a as integer)from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &ConvertExpr{
									Expr: &Column{
										Name: "a",
									},
									Type: IntegerStr,
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "collate",
			stmt:     "SELECT c1 = c2 COLLATE rtrim FROM t",
			deparsed: "select c1=c2 collate rtrim from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
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
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "plus",
			stmt:     "SELECT c1 + 10 FROM t",
			deparsed: "select c1+10 from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &BinaryExpr{
									Operator: PlusStr,
									Left:     &Column{Name: "c1"},
									Right:    &Value{Type: IntValue, Value: []byte("10")},
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "minus",
			stmt:     "SELECT c1 - 10 FROM t",
			deparsed: "select c1-10 from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &BinaryExpr{
									Operator: MinusStr,
									Left:     &Column{Name: "c1"},
									Right:    &Value{Type: IntValue, Value: []byte("10")},
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "multiplication",
			stmt:     "SELECT c1 * 10 FROM t",
			deparsed: "select c1*10 from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &BinaryExpr{
									Operator: MultStr,
									Left:     &Column{Name: "c1"},
									Right:    &Value{Type: IntValue, Value: []byte("10")},
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "division",
			stmt:     "SELECT c1 / 10 FROM t",
			deparsed: "select c1/10 from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &BinaryExpr{
									Operator: DivStr,
									Left:     &Column{Name: "c1"},
									Right:    &Value{Type: IntValue, Value: []byte("10")},
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "mod",
			stmt:     "SELECT c1 % 10 FROM t",
			deparsed: "select c1%10 from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &BinaryExpr{
									Operator: ModStr,
									Left:     &Column{Name: "c1"},
									Right:    &Value{Type: IntValue, Value: []byte("10")},
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "bitand",
			stmt:     "SELECT c1 & 10 FROM t",
			deparsed: "select c1&10 from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &BinaryExpr{
									Operator: BitAndStr,
									Left:     &Column{Name: "c1"},
									Right:    &Value{Type: IntValue, Value: []byte("10")},
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "bitor",
			stmt:     "SELECT c1 | 10 FROM t",
			deparsed: "select c1|10 from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &BinaryExpr{
									Operator: BitOrStr,
									Left:     &Column{Name: "c1"},
									Right:    &Value{Type: IntValue, Value: []byte("10")},
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "leftshift",
			stmt:     "SELECT c1 << 10 FROM t",
			deparsed: "select c1<<10 from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &BinaryExpr{
									Operator: ShiftLeftStr,
									Left:     &Column{Name: "c1"},
									Right:    &Value{Type: IntValue, Value: []byte("10")},
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "rightshift",
			stmt:     "SELECT c1 >> 10 FROM t",
			deparsed: "select c1>>10 from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &BinaryExpr{
									Operator: ShiftRightStr,
									Left:     &Column{Name: "c1"},
									Right:    &Value{Type: IntValue, Value: []byte("10")},
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "concat",
			stmt:     "SELECT c1 || c2 FROM t",
			deparsed: "select c1||c2 from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &BinaryExpr{
									Operator: ConcatStr,
									Left:     &Column{Name: "c1"},
									Right:    &Column{Name: "c2"},
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "json-extract",
			stmt:     "SELECT c1 -> c2 FROM t",
			deparsed: "select c1->c2 from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &BinaryExpr{
									Operator: JSONExtractOp,
									Left:     &Column{Name: "c1"},
									Right:    &Column{Name: "c2"},
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "json-unquote-extract",
			stmt:     "SELECT c1 ->> c2 FROM t",
			deparsed: "select c1->>c2 from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &BinaryExpr{
									Operator: JSONUnquoteExtractOp,
									Left:     &Column{Name: "c1"},
									Right:    &Column{Name: "c2"},
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "bitnot",
			stmt:     "SELECT ~c FROM t",
			deparsed: "select ~c from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &UnaryExpr{
									Operator: TildaStr,
									Expr:     &Column{Name: "c"},
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "unary-plus",
			stmt:     "SELECT +c FROM t",
			deparsed: "select +c from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &UnaryExpr{
									Operator: UPlusStr,
									Expr:     &Column{Name: "c"},
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "between",
			stmt:     "SELECT a BETWEEN b AND c FROM t",
			deparsed: "select a between b and c from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &BetweenExpr{
									Operator: BetweenStr,
									Left:     &Column{Name: "a"},
									From:     &Column{Name: "b"},
									To:       &Column{Name: "c"},
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "between-not",
			stmt:     "SELECT a NOT BETWEEN b AND c FROM t",
			deparsed: "select a not between b and c from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &BetweenExpr{
									Operator: NotBetweenStr,
									Left:     &Column{Name: "a"},
									From:     &Column{Name: "b"},
									To:       &Column{Name: "c"},
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "parens-expr",
			stmt:     "SELECT a and (a and a and (a or a)) FROM t",
			deparsed: "select a and(a and a and(a or a))from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &AndExpr{
									Left: &Column{Name: "a"},
									Right: &ParenExpr{
										Expr: &AndExpr{
											Left: &AndExpr{
												Left:  &Column{Name: "a"},
												Right: &Column{Name: "a"},
											},
											Right: &ParenExpr{
												Expr: &OrExpr{
													Left:  &Column{Name: "a"},
													Right: &Column{Name: "a"},
												},
											},
										},
									},
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "case",
			stmt:     "SELECT CASE c1 WHEN 0 THEN 'zero' WHEN 1 THEN 'one' ELSE 'panic' END FROM t",
			deparsed: "select case c1 when 0 then 'zero' when 1 then 'one' else 'panic' end from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
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
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "case no else",
			stmt:     "SELECT CASE c1 WHEN 0 THEN 'zero' WHEN 1 THEN 'one' END FROM t",
			deparsed: "select case c1 when 0 then 'zero' when 1 then 'one' end from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
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
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "case no expr",
			stmt:     "SELECT CASE WHEN 0 THEN 'zero' WHEN 1 THEN 'one' END FROM t",
			deparsed: "select case when 0 then 'zero' when 1 then 'one' end from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &CaseExpr{
									Expr: nil,
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
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "simple-select",
			stmt:     "SELECT * FROM t WHERE c1 > c2",
			deparsed: "select * from t where c1>c2",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&StarSelectColumn{},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
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
		},
		{
			name:     "multiple-columns",
			stmt:     "SELECT a, t.b bcol, c1 as col, c2 as 'column2', * FROM t WHERE 1",
			deparsed: "select a,t.b as bcol,c1 as col,c2 as 'column2',* from t where 1",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&AliasedSelectColumn{Expr: &Column{Name: "a"}},
							&AliasedSelectColumn{Expr: &Column{Name: "b", TableRef: &Table{Name: "t"}}, As: "bcol"},
							&AliasedSelectColumn{Expr: &Column{Name: "c1"}, As: "col"},
							&AliasedSelectColumn{Expr: &Column{Name: "c2"}, As: "'column2'"},
							&StarSelectColumn{},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},

						Where: &Where{
							Type: WhereStr,
							Expr: &Value{Type: IntValue, Value: []byte("1")},
						},
					},
				},
			},
		},
		{
			name:     "quoted-identifiers-like-drizzle",
			stmt:     `SELECT "t"."a" as "t.a" FROM "t"`,
			deparsed: `select "t"."a" as "t.a" from "t"`,
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&AliasedSelectColumn{Expr: &Column{Name: `"a"`, TableRef: &Table{Name: `"t"`}}, As: `"t.a"`},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: `"t"`, IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "groupby",
			stmt:     "SELECT a, b FROM t GROUP BY a, b",
			deparsed: "select a,b from t group by a,b",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&AliasedSelectColumn{
								Expr: &Column{Name: "a"},
							},
							&AliasedSelectColumn{
								Expr: &Column{Name: "b"},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},

						GroupBy: []Expr{
							&Column{Name: "a"},
							&Column{Name: "b"},
						},
					},
				},
			},
		},
		{
			name:     "groupby-having",
			stmt:     "SELECT a, b FROM t GROUP BY a, b HAVING a = 1",
			deparsed: "select a,b from t group by a,b having a=1",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&AliasedSelectColumn{
								Expr: &Column{Name: "a"},
							},
							&AliasedSelectColumn{
								Expr: &Column{Name: "b"},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
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
		},
		{
			name:     "orderby",
			stmt:     "SELECT a, b FROM t ORDER BY a",
			deparsed: "select a,b from t order by a asc",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&AliasedSelectColumn{
								Expr: &Column{Name: "a"},
							},
							&AliasedSelectColumn{
								Expr: &Column{Name: "b"},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},

						OrderBy: OrderBy{
							&OrderingTerm{Expr: &Column{Name: "a"}, Direction: AscStr},
						},
					},
				},
			},
		},
		{
			name:     "orderby-asc",
			stmt:     "SELECT a, b FROM t ORDER BY a asc",
			deparsed: "select a,b from t order by a asc",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&AliasedSelectColumn{
								Expr: &Column{Name: "a"},
							},
							&AliasedSelectColumn{
								Expr: &Column{Name: "b"},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},

						OrderBy: OrderBy{
							&OrderingTerm{Expr: &Column{Name: "a"}, Direction: AscStr},
						},
					},
				},
			},
		},
		{
			name:     "orderby-asc",
			stmt:     "SELECT a, b FROM t ORDER BY a desc",
			deparsed: "select a,b from t order by a desc",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&AliasedSelectColumn{
								Expr: &Column{Name: "a"},
							},
							&AliasedSelectColumn{
								Expr: &Column{Name: "b"},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},

						OrderBy: OrderBy{
							&OrderingTerm{Expr: &Column{Name: "a"}, Direction: DescStr},
						},
					},
				},
			},
		},
		{
			name:     "orderby-desc-asc",
			stmt:     "SELECT a, b FROM t ORDER BY a desc, b",
			deparsed: "select a,b from t order by a desc,b asc",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&AliasedSelectColumn{
								Expr: &Column{Name: "a"},
							},
							&AliasedSelectColumn{
								Expr: &Column{Name: "b"},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},

						OrderBy: OrderBy{
							&OrderingTerm{Expr: &Column{Name: "a"}, Direction: DescStr},
							&OrderingTerm{Expr: &Column{Name: "b"}, Direction: AscStr},
						},
					},
				},
			},
		},
		{
			name:     "orderby-nulls",
			stmt:     "SELECT a, b, c FROM t ORDER BY a desc, b NULLS FIRST, c NULLS LAST",
			deparsed: "select a,b,c from t order by a desc,b asc nulls first,c asc nulls last",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
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
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},

						OrderBy: OrderBy{
							&OrderingTerm{Expr: &Column{Name: "a"}, Direction: DescStr, Nulls: NullsNil},
							&OrderingTerm{Expr: &Column{Name: "b"}, Direction: AscStr, Nulls: NullsFirst},
							&OrderingTerm{Expr: &Column{Name: "c"}, Direction: AscStr, Nulls: NullsLast},
						},
					},
				},
			},
		},
		{
			name:     "limit",
			stmt:     "SELECT * FROM t LIMIT 1",
			deparsed: "select * from t limit 1",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&StarSelectColumn{},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},

						Limit: &Limit{
							Limit: &Value{Type: IntValue, Value: []byte("1")},
						},
					},
				},
			},
		},
		{
			name:     "limit-offet",
			stmt:     "SELECT * FROM t LIMIT 1 OFFSET 2",
			deparsed: "select * from t limit 1 offset 2",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&StarSelectColumn{},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},

						Limit: &Limit{
							Limit:  &Value{Type: IntValue, Value: []byte("1")},
							Offset: &Value{Type: IntValue, Value: []byte("2")},
						},
					},
				},
			},
		},
		{
			name:     "limit-offet-alternative",
			stmt:     "SELECT * FROM t LIMIT 1, 2",
			deparsed: "select * from t limit 2 offset 1",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&StarSelectColumn{},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},

						Limit: &Limit{
							Limit:  &Value{Type: IntValue, Value: []byte("2")},
							Offset: &Value{Type: IntValue, Value: []byte("1")},
						},
					},
				},
			},
		},
		{
			name:     "simple-select-distinct",
			stmt:     "SELECT DISTINCT * FROM t",
			deparsed: "select distinct * from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						Distinct: DistinctStr,
						SelectColumnList: SelectColumnList{
							&StarSelectColumn{},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "simple-select-all",
			stmt:     "SELECT ALL * FROM t",
			deparsed: "select all * from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						Distinct: AllStr,
						SelectColumnList: SelectColumnList{
							&StarSelectColumn{},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "simple-select-alias-table",
			stmt:     "SELECT * FROM t as t",
			deparsed: "select * from t as t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&StarSelectColumn{},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
							As:   "t",
						},
					},
				},
			},
		},
		{
			name:     "simple-select-alias-table-alt",
			stmt:     "SELECT * FROM t t",
			deparsed: "select * from t as t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&StarSelectColumn{},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
							As:   "t",
						},
					},
				},
			},
		},
		{
			name:     "simple-select-alias-table-alt-string",
			stmt:     "SELECT * FROM t 't'",
			deparsed: "select * from t as 't'",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&StarSelectColumn{},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
							As:   "'t'",
						},
					},
				},
			},
		},
		{
			name:     "select-multiple-tables",
			stmt:     "SELECT t.*, t2.c1 as column1 FROM t, t2",
			deparsed: "select t.*,t2.c1 as column1 from t join t2",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&StarSelectColumn{TableRef: &Table{Name: "t"}},
							&AliasedSelectColumn{
								Expr: &Column{Name: "c1", TableRef: &Table{Name: "t2"}},
								As:   "column1",
							},
						},
						From: &JoinTableExpr{
							LeftExpr: &AliasedTableExpr{
								Expr: &Table{Name: "t", IsTarget: true},
							},
							JoinOperator: &JoinOperator{Op: JoinStr},
							RightExpr: &AliasedTableExpr{
								Expr: &Table{Name: "t2", IsTarget: true},
							},
						},
					},
				},
			},
		},
		{
			name:     "select-from-subquery",
			stmt:     "SELECT * FROM (SELECT * FROM t)",
			deparsed: "select * from(select * from t)",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&StarSelectColumn{},
						},
						From: &AliasedTableExpr{
							Expr: &Subquery{
								Select: &Select{
									SelectColumnList: SelectColumnList{
										&StarSelectColumn{},
									},
									From: &AliasedTableExpr{
										Expr: &Table{Name: "t", IsTarget: true},
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
			stmt:     "SELECT * FROM (SELECT * FROM t) as subquery",
			deparsed: "select * from(select * from t)as subquery",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&StarSelectColumn{},
						},
						From: &AliasedTableExpr{
							Expr: &Subquery{
								Select: &Select{
									SelectColumnList: SelectColumnList{
										&StarSelectColumn{},
									},
									From: &AliasedTableExpr{
										Expr: &Table{Name: "t", IsTarget: true},
									},
								},
							},
							As: "subquery",
						},
					},
				},
			},
		},
		{
			name:     "select-from-subquery-aliased-alt",
			stmt:     "SELECT * FROM (SELECT * FROM t) subquery",
			deparsed: "select * from(select * from t)as subquery",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&StarSelectColumn{},
						},
						From: &AliasedTableExpr{
							Expr: &Subquery{
								Select: &Select{
									SelectColumnList: SelectColumnList{
										&StarSelectColumn{},
									},
									From: &AliasedTableExpr{
										Expr: &Table{Name: "t", IsTarget: true},
									},
								},
							},
							As: "subquery",
						},
					},
				},
			},
		},
		{
			name:     "join",
			stmt:     "SELECT * FROM t JOIN t2 JOIN t3 JOIN t4",
			deparsed: "select * from t join t2 join t3 join t4",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&StarSelectColumn{},
						},
						From: &JoinTableExpr{
							LeftExpr: &JoinTableExpr{
								LeftExpr: &JoinTableExpr{
									LeftExpr:     &AliasedTableExpr{Expr: &Table{Name: "t", IsTarget: true}},
									JoinOperator: &JoinOperator{Op: JoinStr},
									RightExpr:    &AliasedTableExpr{Expr: &Table{Name: "t2", IsTarget: true}},
								},
								JoinOperator: &JoinOperator{Op: JoinStr},
								RightExpr:    &AliasedTableExpr{Expr: &Table{Name: "t3", IsTarget: true}},
							},
							JoinOperator: &JoinOperator{Op: JoinStr},
							RightExpr:    &AliasedTableExpr{Expr: &Table{Name: "t4", IsTarget: true}},
						},
					},
				},
			},
		},
		{
			name:     "cross join",
			stmt:     "SELECT * FROM t CROSS JOIN t2",
			deparsed: "select * from t join t2",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&StarSelectColumn{},
						},
						From: &JoinTableExpr{
							LeftExpr:     &AliasedTableExpr{Expr: &Table{Name: "t", IsTarget: true}},
							JoinOperator: &JoinOperator{Op: JoinStr},
							RightExpr:    &AliasedTableExpr{Expr: &Table{Name: "t2", IsTarget: true}},
						},
					},
				},
			},
		},
		{
			name:     "left join",
			stmt:     "SELECT * FROM t LEFT JOIN t2",
			deparsed: "select * from t left join t2",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&StarSelectColumn{},
						},
						From: &JoinTableExpr{
							LeftExpr:     &AliasedTableExpr{Expr: &Table{Name: "t", IsTarget: true}},
							JoinOperator: &JoinOperator{Op: LeftJoinStr},
							RightExpr:    &AliasedTableExpr{Expr: &Table{Name: "t2", IsTarget: true}},
						},
					},
				},
			},
		},
		{
			name:     "right join",
			stmt:     "SELECT * FROM t RIGHT JOIN t2",
			deparsed: "select * from t right join t2",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&StarSelectColumn{},
						},
						From: &JoinTableExpr{
							LeftExpr:     &AliasedTableExpr{Expr: &Table{Name: "t", IsTarget: true}},
							JoinOperator: &JoinOperator{Op: RightJoinStr},
							RightExpr:    &AliasedTableExpr{Expr: &Table{Name: "t2", IsTarget: true}},
						},
					},
				},
			},
		},
		{
			name:     "full join",
			stmt:     "SELECT * FROM t FULL JOIN t2",
			deparsed: "select * from t full join t2",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&StarSelectColumn{},
						},
						From: &JoinTableExpr{
							LeftExpr:     &AliasedTableExpr{Expr: &Table{Name: "t", IsTarget: true}},
							JoinOperator: &JoinOperator{Op: FullJoinStr},
							RightExpr:    &AliasedTableExpr{Expr: &Table{Name: "t2", IsTarget: true}},
						},
					},
				},
			},
		},
		{
			name:     "inner join",
			stmt:     "SELECT * FROM t INNER JOIN t2",
			deparsed: "select * from t inner join t2",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&StarSelectColumn{},
						},
						From: &JoinTableExpr{
							LeftExpr:     &AliasedTableExpr{Expr: &Table{Name: "t", IsTarget: true}},
							JoinOperator: &JoinOperator{Op: InnerJoinStr},
							RightExpr:    &AliasedTableExpr{Expr: &Table{Name: "t2", IsTarget: true}},
						},
					},
				},
			},
		},
		{
			name:     "natural left join",
			stmt:     "SELECT * FROM t NATURAL LEFT JOIN t2",
			deparsed: "select * from t natural left join t2",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&StarSelectColumn{},
						},
						From: &JoinTableExpr{
							LeftExpr:     &AliasedTableExpr{Expr: &Table{Name: "t", IsTarget: true}},
							JoinOperator: &JoinOperator{Op: LeftJoinStr, Natural: true},
							RightExpr:    &AliasedTableExpr{Expr: &Table{Name: "t2", IsTarget: true}},
						},
					},
				},
			},
		},
		{
			name:     "natural right join",
			stmt:     "SELECT * FROM t NATURAL RIGHT JOIN t2",
			deparsed: "select * from t natural right join t2",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&StarSelectColumn{},
						},
						From: &JoinTableExpr{
							LeftExpr:     &AliasedTableExpr{Expr: &Table{Name: "t", IsTarget: true}},
							JoinOperator: &JoinOperator{Op: RightJoinStr, Natural: true},
							RightExpr:    &AliasedTableExpr{Expr: &Table{Name: "t2", IsTarget: true}},
						},
					},
				},
			},
		},
		{
			name:     "natural full join",
			stmt:     "SELECT * FROM t NATURAL FULL JOIN t2",
			deparsed: "select * from t natural full join t2",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&StarSelectColumn{},
						},
						From: &JoinTableExpr{
							LeftExpr:     &AliasedTableExpr{Expr: &Table{Name: "t", IsTarget: true}},
							JoinOperator: &JoinOperator{Op: FullJoinStr, Natural: true},
							RightExpr:    &AliasedTableExpr{Expr: &Table{Name: "t2", IsTarget: true}},
						},
					},
				},
			},
		},
		{
			name:     "natural inner join",
			stmt:     "SELECT * FROM t NATURAL INNER JOIN t2",
			deparsed: "select * from t natural inner join t2",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&StarSelectColumn{},
						},
						From: &JoinTableExpr{
							LeftExpr:     &AliasedTableExpr{Expr: &Table{Name: "t", IsTarget: true}},
							JoinOperator: &JoinOperator{Op: InnerJoinStr, Natural: true},
							RightExpr:    &AliasedTableExpr{Expr: &Table{Name: "t2", IsTarget: true}},
						},
					},
				},
			},
		},
		{
			name:     "left join outer",
			stmt:     "SELECT * FROM t LEFT OUTER JOIN t2",
			deparsed: "select * from t left outer join t2",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&StarSelectColumn{},
						},
						From: &JoinTableExpr{
							LeftExpr:     &AliasedTableExpr{Expr: &Table{Name: "t", IsTarget: true}},
							JoinOperator: &JoinOperator{Op: LeftJoinStr, Outer: true},
							RightExpr:    &AliasedTableExpr{Expr: &Table{Name: "t2", IsTarget: true}},
						},
					},
				},
			},
		},
		{
			name:     "right join outer",
			stmt:     "SELECT * FROM t RIGHT OUTER JOIN t2",
			deparsed: "select * from t right outer join t2",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&StarSelectColumn{},
						},
						From: &JoinTableExpr{
							LeftExpr:     &AliasedTableExpr{Expr: &Table{Name: "t", IsTarget: true}},
							JoinOperator: &JoinOperator{Op: RightJoinStr, Outer: true},
							RightExpr:    &AliasedTableExpr{Expr: &Table{Name: "t2", IsTarget: true}},
						},
					},
				},
			},
		},
		{
			name:     "full join outer",
			stmt:     "SELECT * FROM t FULL OUTER JOIN t2",
			deparsed: "select * from t full outer join t2",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&StarSelectColumn{},
						},
						From: &JoinTableExpr{
							LeftExpr:     &AliasedTableExpr{Expr: &Table{Name: "t", IsTarget: true}},
							JoinOperator: &JoinOperator{Op: FullJoinStr, Outer: true},
							RightExpr:    &AliasedTableExpr{Expr: &Table{Name: "t2", IsTarget: true}},
						},
					},
				},
			},
		},
		{
			name:     "join-on",
			stmt:     "SELECT * FROM t JOIN t2 ON t.a = t2.a JOIN t3 ON t2.c1 = t3.c1",
			deparsed: "select * from t join t2 on t.a=t2.a join t3 on t2.c1=t3.c1",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&StarSelectColumn{},
						},
						From: &JoinTableExpr{
							LeftExpr: &JoinTableExpr{
								LeftExpr:     &AliasedTableExpr{Expr: &Table{Name: "t", IsTarget: true}},
								JoinOperator: &JoinOperator{Op: JoinStr},
								RightExpr:    &AliasedTableExpr{Expr: &Table{Name: "t2", IsTarget: true}},
								On: &CmpExpr{
									Operator: EqualStr,
									Left:     &Column{Name: "a", TableRef: &Table{Name: "t"}},
									Right:    &Column{Name: "a", TableRef: &Table{Name: "t2"}},
								},
							},
							JoinOperator: &JoinOperator{Op: JoinStr},
							RightExpr:    &AliasedTableExpr{Expr: &Table{Name: "t3", IsTarget: true}},
							On: &CmpExpr{
								Operator: EqualStr,
								Left:     &Column{Name: "c1", TableRef: &Table{Name: "t2"}},
								Right:    &Column{Name: "c1", TableRef: &Table{Name: "t3"}},
							},
						},
					},
				},
			},
		},
		{
			name:     "join-using",
			stmt:     "SELECT * FROM t JOIN t2 USING (c1, c2)",
			deparsed: "select * from t join t2 using(c1,c2)",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&StarSelectColumn{},
						},
						From: &JoinTableExpr{
							LeftExpr:     &AliasedTableExpr{Expr: &Table{Name: "t", IsTarget: true}},
							JoinOperator: &JoinOperator{Op: JoinStr},
							RightExpr:    &AliasedTableExpr{Expr: &Table{Name: "t2", IsTarget: true}},
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
			name:     "table expr parenthesis join",
			stmt:     "SELECT * FROM (t JOIN t2)",
			deparsed: "select * from t join t2",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&StarSelectColumn{},
						},
						From: &JoinTableExpr{
							LeftExpr:     &AliasedTableExpr{Expr: &Table{Name: "t", IsTarget: true}},
							JoinOperator: &JoinOperator{Op: JoinStr},
							RightExpr:    &AliasedTableExpr{Expr: &Table{Name: "t2", IsTarget: true}},
						},
					},
				},
			},
		},
		{
			name:     "table expr parenthesis",
			stmt:     "SELECT * FROM (t)",
			deparsed: "select * from(t)",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&StarSelectColumn{},
						},
						From: &ParenTableExpr{
							TableExpr: &AliasedTableExpr{
								Expr: &Table{Name: "t", IsTarget: true},
							},
						},
					},
				},
			},
		},
		{
			name:     "subquery",
			stmt:     "SELECT * FROM t WHERE (SELECT 1 FROM t2)",
			deparsed: "select * from t where (select 1 from t2)",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&StarSelectColumn{},
						},
						From: &AliasedTableExpr{Expr: &Table{Name: "t", IsTarget: true}},
						Where: &Where{
							Type: WhereStr,
							Expr: &Subquery{
								Select: &Select{
									SelectColumnList: SelectColumnList{
										&AliasedSelectColumn{
											Expr: &Value{Type: IntValue, Value: []byte("1")},
										},
									},
									From: &AliasedTableExpr{Expr: &Table{Name: "t2", IsTarget: true}},
								},
							},
						},
					},
				},
			},
		},
		{
			name:     "exists",
			stmt:     "SELECT * FROM t WHERE EXISTS (SELECT 1 FROM t2)",
			deparsed: "select * from t where exists(select 1 from t2)",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&StarSelectColumn{},
						},
						From: &AliasedTableExpr{Expr: &Table{Name: "t", IsTarget: true}},
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
										From: &AliasedTableExpr{Expr: &Table{Name: "t2", IsTarget: true}},
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
			stmt:     "SELECT * FROM t WHERE NOT EXISTS (SELECT 1 FROM t2)",
			deparsed: "select * from t where not exists(select 1 from t2)",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&StarSelectColumn{},
						},
						From: &AliasedTableExpr{Expr: &Table{Name: "t", IsTarget: true}},
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
											From: &AliasedTableExpr{Expr: &Table{Name: "t2", IsTarget: true}},
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
			name:     "in empty",
			stmt:     "SELECT a FROM t WHERE a IN ()",
			deparsed: "select a from t where a in()",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&AliasedSelectColumn{
								Expr: &Column{Name: "a"},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},

						Where: &Where{
							Type: WhereStr,
							Expr: &CmpExpr{
								Operator: InStr,
								Left:     &Column{Name: "a"},
								Right:    Exprs{},
							},
						},
					},
				},
			},
		},
		{
			name:     "in multiple values",
			stmt:     "SELECT a FROM t WHERE a IN (1, 2)",
			deparsed: "select a from t where a in(1,2)",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&AliasedSelectColumn{
								Expr: &Column{Name: "a"},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},

						Where: &Where{
							Type: WhereStr,
							Expr: &CmpExpr{
								Operator: InStr,
								Left:     &Column{Name: "a"},
								Right: Exprs{
									&Value{Type: IntValue, Value: []byte("1")},
									&Value{Type: IntValue, Value: []byte("2")},
								},
							},
						},
					},
				},
			},
		},
		{
			name:     "in subselect",
			stmt:     "SELECT a FROM t WHERE a IN(SELECT a FROM t2)",
			deparsed: "select a from t where a in(select a from t2)",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&AliasedSelectColumn{
								Expr: &Column{Name: "a"},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},

						Where: &Where{
							Type: WhereStr,
							Expr: &CmpExpr{
								Operator: InStr,
								Left:     &Column{Name: "a"},
								Right: &Subquery{
									Select: &Select{
										SelectColumnList: SelectColumnList{
											&AliasedSelectColumn{
												Expr: &Column{Name: "a"},
											},
										},
										From: &AliasedTableExpr{
											Expr: &Table{Name: "t2", IsTarget: true},
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
			name:     "not in empty",
			stmt:     "SELECT a FROM t WHERE a NOT IN ()",
			deparsed: "select a from t where a not in()",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&AliasedSelectColumn{
								Expr: &Column{Name: "a"},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},

						Where: &Where{
							Type: WhereStr,
							Expr: &CmpExpr{
								Operator: NotInStr,
								Left:     &Column{Name: "a"},
								Right:    Exprs{},
							},
						},
					},
				},
			},
		},
		{
			name:     "not in multiple values",
			stmt:     "SELECT a FROM t WHERE a NOT IN (1, 2)",
			deparsed: "select a from t where a not in(1,2)",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&AliasedSelectColumn{
								Expr: &Column{Name: "a"},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},

						Where: &Where{
							Type: WhereStr,
							Expr: &CmpExpr{
								Operator: NotInStr,
								Left:     &Column{Name: "a"},
								Right: Exprs{
									&Value{Type: IntValue, Value: []byte("1")},
									&Value{Type: IntValue, Value: []byte("2")},
								},
							},
						},
					},
				},
			},
		},
		{
			name:     "not in subselect",
			stmt:     "SELECT a FROM t WHERE a NOT IN (SELECT a FROM t2)",
			deparsed: "select a from t where a not in(select a from t2)",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&AliasedSelectColumn{
								Expr: &Column{Name: "a"},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},

						Where: &Where{
							Type: WhereStr,
							Expr: &CmpExpr{
								Operator: NotInStr,
								Left:     &Column{Name: "a"},
								Right: &Subquery{
									Select: &Select{
										SelectColumnList: SelectColumnList{
											&AliasedSelectColumn{
												Expr: &Column{Name: "a"},
											},
										},
										From: &AliasedTableExpr{
											Expr: &Table{Name: "t2", IsTarget: true},
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
			name:     "function call",
			stmt:     "SELECT count(c1) FROM t",
			deparsed: "select count(c1)from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&AliasedSelectColumn{
								Expr: &FuncExpr{
									Name: "count",
									Args: Exprs{
										&Column{Name: "c1"},
									},
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "function call filter",
			stmt:     "SELECT max(a) FILTER(WHERE a > 2) FROM t",
			deparsed: "select max(a)filter(where a>2)from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&AliasedSelectColumn{
								Expr: &FuncExpr{
									Name: "max",
									Args: Exprs{
										&Column{Name: "a"},
									},
									Filter: &Where{
										Type: WhereStr,
										Expr: &CmpExpr{
											Operator: GreaterThanStr,
											Left:     &Column{Name: "a"},
											Right:    &Value{Type: IntValue, Value: []byte("2")},
										},
									},
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "function call",
			stmt:     "SELECT count(c1) FROM t",
			deparsed: "select count(c1)from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&AliasedSelectColumn{
								Expr: &FuncExpr{
									Name: "count",
									Args: Exprs{
										&Column{Name: "c1"},
									},
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "function call upper",
			stmt:     "SELECT COUNT(c1) FROM t",
			deparsed: "select count(c1)from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&AliasedSelectColumn{
								Expr: &FuncExpr{
									Name: "count",
									Args: Exprs{
										&Column{Name: "c1"},
									},
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "function call star",
			stmt:     "SELECT count(*) FROM t",
			deparsed: "select count(*)from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&AliasedSelectColumn{
								Expr: &FuncExpr{
									Name: "count",
									Args: nil,
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "function call star upper",
			stmt:     "SELECT COUNT(*) FROM t",
			deparsed: "select count(*)from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&AliasedSelectColumn{
								Expr: &FuncExpr{
									Name: "count",
									Args: nil,
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:        "function does not exist star",
			stmt:        "SELECT foo(*) FROM t",
			deparsed:    "select foo(*)from t",
			expectedAST: nil,
			expectedErr: &ErrNoSuchFunction{FunctionName: "foo"},
		},
		{
			name:     "function call distinct",
			stmt:     "SELECT count(distinct c1) FROM t",
			deparsed: "select count(distinct c1)from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&AliasedSelectColumn{
								Expr: &FuncExpr{
									Distinct: true,
									Name:     "count",
									Args: Exprs{
										&Column{Name: "c1"},
									},
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:        "function does not exist",
			stmt:        "SELECT foo(ID) FILTER(WHERE ID > 2) FROM t",
			deparsed:    "select foo(ID)filter(where ID>2)from t",
			expectedAST: nil,
			expectedErr: &ErrNoSuchFunction{FunctionName: "foo"},
		},
		{
			name:     "function call like with escape",
			stmt:     "SELECT like(a, b, c) FROM t",
			deparsed: "select like(a,b,c)from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&AliasedSelectColumn{
								Expr: &FuncExpr{
									Name: "like",
									Args: Exprs{
										&Column{Name: "a"},
										&Column{Name: "b"},
										&Column{Name: "c"},
									},
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		},
		{
			name:     "identifier delimiters",
			stmt:     "SELECT t. a, `t2`.`b`, \"t3\".\"c\", [t4].[a]  FROM t JOIN `t2` JOIN \"t3\" JOIN [t4]",
			deparsed: "select t.a,`t2`.`b`,\"t3\".\"c\",[t4].[a] from t join `t2` join \"t3\" join [t4]",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&AliasedSelectColumn{
								Expr: &Column{
									Name: Identifier("a"),
									TableRef: &Table{
										Name: Identifier("t"),
									},
								},
							},
							&AliasedSelectColumn{
								Expr: &Column{
									Name: Identifier("`b`"),
									TableRef: &Table{
										Name: Identifier("`t2`"),
									},
								},
							},
							&AliasedSelectColumn{
								Expr: &Column{
									Name: Identifier("\"c\""),
									TableRef: &Table{
										Name: Identifier("\"t3\""),
									},
								},
							},
							&AliasedSelectColumn{
								Expr: &Column{
									Name: Identifier("[a]"),
									TableRef: &Table{
										Name: Identifier("[t4]"),
									},
								},
							},
						},
						From: &JoinTableExpr{
							JoinOperator: &JoinOperator{Op: JoinStr},
							LeftExpr: &JoinTableExpr{
								JoinOperator: &JoinOperator{Op: JoinStr},
								LeftExpr: &JoinTableExpr{
									JoinOperator: &JoinOperator{Op: JoinStr},
									LeftExpr: &AliasedTableExpr{
										Expr: &Table{Name: Identifier("t"), IsTarget: true},
									},
									RightExpr: &AliasedTableExpr{
										Expr: &Table{Name: Identifier("`t2`"), IsTarget: true},
									},
								},
								RightExpr: &AliasedTableExpr{
									Expr: &Table{Name: Identifier("\"t3\""), IsTarget: true},
								},
							},
							RightExpr: &AliasedTableExpr{
								Expr: &Table{Name: Identifier("[t4]"), IsTarget: true},
							},
						},
					},
				},
			},
		},
		{
			name:     "select union",
			stmt:     "SELECT a FROM t UNION SELECT a FROM t2",
			deparsed: "select a from t union select a from t2",
			expectedAST: &AST{
				Statements: []Statement{
					&CompoundSelect{
						Left: &Select{
							SelectColumnList: SelectColumnList{
								&AliasedSelectColumn{
									Expr: &Column{Name: "a"},
								},
							},
							From: &AliasedTableExpr{
								Expr: &Table{Name: "t", IsTarget: true},
							},
						},
						Type: CompoundUnionStr,
						Right: &Select{
							SelectColumnList: SelectColumnList{
								&AliasedSelectColumn{
									Expr: &Column{Name: "a"},
								},
							},
							From: &AliasedTableExpr{
								Expr: &Table{Name: "t2", IsTarget: true},
							},
						},
					},
				},
			},
		},
		{
			name:     "select union all",
			stmt:     "SELECT a FROM t UNION ALL SELECT a FROM t2",
			deparsed: "select a from t union all select a from t2",
			expectedAST: &AST{
				Statements: []Statement{
					&CompoundSelect{
						Left: &Select{
							SelectColumnList: SelectColumnList{
								&AliasedSelectColumn{
									Expr: &Column{Name: "a"},
								},
							},
							From: &AliasedTableExpr{
								Expr: &Table{Name: "t", IsTarget: true},
							},
						},
						Type: CompoundUnionAllStr,
						Right: &Select{
							SelectColumnList: SelectColumnList{
								&AliasedSelectColumn{
									Expr: &Column{Name: "a"},
								},
							},
							From: &AliasedTableExpr{
								Expr: &Table{Name: "t2", IsTarget: true},
							},
						},
					},
				},
			},
		},
		{
			name:     "select except",
			stmt:     "SELECT a FROM t EXCEPT SELECT a FROM t2",
			deparsed: "select a from t except select a from t2",
			expectedAST: &AST{
				Statements: []Statement{
					&CompoundSelect{
						Left: &Select{
							SelectColumnList: SelectColumnList{
								&AliasedSelectColumn{
									Expr: &Column{Name: "a"},
								},
							},
							From: &AliasedTableExpr{
								Expr: &Table{Name: "t", IsTarget: true},
							},
						},
						Type: CompoundExceptStr,
						Right: &Select{
							SelectColumnList: SelectColumnList{
								&AliasedSelectColumn{
									Expr: &Column{Name: "a"},
								},
							},
							From: &AliasedTableExpr{
								Expr: &Table{Name: "t2", IsTarget: true},
							},
						},
					},
				},
			},
		},
		{
			name:     "select intersect",
			stmt:     "SELECT a FROM t INTERSECT SELECT a FROM t2 ORDER BY a",
			deparsed: "select a from t intersect select a from t2 order by a asc",
			expectedAST: &AST{
				Statements: []Statement{
					&CompoundSelect{
						Left: &Select{
							SelectColumnList: SelectColumnList{
								&AliasedSelectColumn{
									Expr: &Column{Name: "a"},
								},
							},
							From: &AliasedTableExpr{
								Expr: &Table{Name: "t", IsTarget: true},
							},
						},
						Type: CompoundIntersectStr,
						Right: &Select{
							SelectColumnList: SelectColumnList{
								&AliasedSelectColumn{
									Expr: &Column{Name: "a"},
								},
							},
							From: &AliasedTableExpr{
								Expr: &Table{Name: "t2", IsTarget: true},
							},
						},
						OrderBy: []*OrderingTerm{
							{
								Expr:      &Column{Name: "a"},
								Direction: AscStr,
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
				if tc.expectedErr == nil {
					require.NoError(t, err)
					require.Len(t, ast.Errors, 0)
					require.Equal(t, tc.expectedAST, ast)
					require.Equal(t, tc.deparsed, ast.String())

					// test all SELECT statements against SQLite3
					db, err := sql.Open("sqlite3", "file::"+uuid.NewString()+":?mode=memory&cache=shared&_foreign_keys=on")
					require.NoError(t, err)

					// create dummy tables
					_, err = db.Exec(`
					    CREATE TABLE t (
							a int, b int, c int, c1 int, c2 int, thisisacolumn int, this_is_a_column3208ADKJHKDS_ int, _also_column int
						);
						CREATE TABLE t2 (a int, b int, c int, c1 int, c2 int);
						CREATE TABLE t3 (a int, b int, c int, c1 int, c2 int);
						CREATE TABLE t4 (a int, b int, c int, c1 int, c2 int);
					`)
					require.NoError(t, err)

					_, err = db.Exec(tc.stmt)
					require.NoError(t, err)
					require.NoError(t, db.Close())
				} else {
					require.ErrorAs(t, ast.Errors[0], &tc.expectedErr)
				}
			}
		}(tc))
	}
}

func TestAllowedFunctions(t *testing.T) {
	t.Parallel()

	genFunctionCallAndArgs := func(fname string) (string, Exprs) {
		switch fname {
		case "like":
			return "like(a,b)", Exprs{
				&Column{Name: "a"},
				&Column{Name: "b"},
			}
		case "glob":
			return "glob(a,b)", Exprs{
				&Column{Name: "a"},
				&Column{Name: "b"},
			}
		default:
			return fmt.Sprintf("%s(*)", fname), nil
		}
	}

	type testCase struct {
		name        string
		stmt        string
		deparsed    string
		expectedAST *AST
	}

	tests := []testCase{}
	for allowedFunction, isCustom := range AllowedFunctions {
		if isCustom {
			continue
		}
		functionCall, args := genFunctionCallAndArgs(allowedFunction)
		tests = append(tests, testCase{
			name:     allowedFunction,
			stmt:     fmt.Sprintf("select %s from t", functionCall),
			deparsed: fmt.Sprintf("select %sfrom t", functionCall),
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&AliasedSelectColumn{
								Expr: &FuncExpr{
									Name: Identifier(allowedFunction),
									Args: args,
								},
							},
						},
						From: &AliasedTableExpr{
							Expr: &Table{Name: "t", IsTarget: true},
						},
					},
				},
			},
		})
	}

	for _, tc := range tests {
		t.Run(tc.name, func(tc testCase) func(t *testing.T) {
			return func(t *testing.T) {
				t.Parallel()
				ast, err := Parse(tc.stmt)
				require.NoError(t, err)
				require.Len(t, ast.Errors, 0)
				require.Equal(t, tc.expectedAST, ast)
				require.Equal(t, tc.deparsed, ast.String())
			}
		}(tc))
	}
}

func TestCreateTable(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name         string
		stmt         string
		deparsed     string
		expectedAST  *AST
		expectedHash string
	}

	tests := []testCase{
		{
			name:         "create table simple",
			stmt:         "CREATE TABLE t (a INT);",
			deparsed:     "create table t(a int)",
			expectedHash: "0605f6c6705c7c1257edb2d61d94a03ad15f1d253a5a75525c6da8cda34a99ee",
			expectedAST: &AST{
				Statements: []Statement{
					&CreateTable{
						Table:       &Table{Name: "t", IsTarget: true},
						Constraints: []TableConstraint{},
						ColumnsDef: []*ColumnDef{
							{Column: &Column{Name: "a"}, Type: TypeIntStr, Constraints: []ColumnConstraint{}},
						},
					},
				},
			},
		},
		{
			name:         "create table backtick",
			stmt:         "CREATE TABLE `t` (a INT);",
			deparsed:     "create table `t`(a int)",
			expectedHash: "0605f6c6705c7c1257edb2d61d94a03ad15f1d253a5a75525c6da8cda34a99ee",
			expectedAST: &AST{
				Statements: []Statement{
					&CreateTable{
						Table:       &Table{Name: "`t`", IsTarget: true},
						Constraints: []TableConstraint{},
						ColumnsDef: []*ColumnDef{
							{Column: &Column{Name: "a"}, Type: TypeIntStr, Constraints: []ColumnConstraint{}},
						},
					},
				},
			},
		},
		{
			name:         "create table double quotes",
			stmt:         "CREATE TABLE \"t\" (a INT);",
			deparsed:     "create table \"t\"(a int)",
			expectedHash: "0605f6c6705c7c1257edb2d61d94a03ad15f1d253a5a75525c6da8cda34a99ee",
			expectedAST: &AST{
				Statements: []Statement{
					&CreateTable{
						Table:       &Table{Name: "\"t\"", IsTarget: true},
						Constraints: []TableConstraint{},
						ColumnsDef: []*ColumnDef{
							{Column: &Column{Name: "a"}, Type: TypeIntStr, Constraints: []ColumnConstraint{}},
						},
					},
				},
			},
		},
		{
			name:         "create table square brackets",
			stmt:         "CREATE TABLE [t] (a INT);",
			deparsed:     "create table [t](a int)",
			expectedHash: "0605f6c6705c7c1257edb2d61d94a03ad15f1d253a5a75525c6da8cda34a99ee",
			expectedAST: &AST{
				Statements: []Statement{
					&CreateTable{
						Table:       &Table{Name: "[t]", IsTarget: true},
						Constraints: []TableConstraint{},
						ColumnsDef: []*ColumnDef{
							{Column: &Column{Name: "a"}, Type: TypeIntStr, Constraints: []ColumnConstraint{}},
						},
					},
				},
			},
		},
		{
			name:         "create table types",
			stmt:         "CREATE TABLE t (a INT, b INTEGER, c TEXT, d BLOB);",
			deparsed:     "create table t(a int,b integer,c text,d blob)",
			expectedHash: "4fe547ac5242c1f0f98a5918a570b498574f95389dc7bf59fd4eabe765938a03",
			expectedAST: &AST{
				Statements: []Statement{
					&CreateTable{
						Table:       &Table{Name: "t", IsTarget: true},
						Constraints: []TableConstraint{},
						ColumnsDef: []*ColumnDef{
							{Column: &Column{Name: "a"}, Type: TypeIntStr, Constraints: []ColumnConstraint{}},
							{Column: &Column{Name: "b"}, Type: TypeIntegerStr, Constraints: []ColumnConstraint{}},
							{Column: &Column{Name: "c"}, Type: TypeTextStr, Constraints: []ColumnConstraint{}},
							{Column: &Column{Name: "d"}, Type: TypeBlobStr, Constraints: []ColumnConstraint{}},
						},
					},
				},
			},
		},
		{
			name:         "create table primary key",
			stmt:         "CREATE TABLE t (id INT PRIMARY KEY, a INT);",
			deparsed:     "create table t(id int primary key,a int)",
			expectedHash: "a360ddb202c0871558c0a3140a67e8a7a7a76e794a297214f7443e8739546408",
			expectedAST: &AST{
				Statements: []Statement{
					&CreateTable{
						Table:       &Table{Name: "t", IsTarget: true},
						Constraints: []TableConstraint{},
						ColumnsDef: []*ColumnDef{
							{
								Column: &Column{Name: "id"},
								Type:   TypeIntStr,
								Constraints: []ColumnConstraint{
									&ColumnConstraintPrimaryKey{Order: PrimaryKeyOrderEmpty},
								},
							},
							{Column: &Column{Name: "a"}, Type: TypeIntStr, Constraints: []ColumnConstraint{}},
						},
					},
				},
			},
		},
		{
			name:         "create table primary key asc",
			stmt:         "CREATE TABLE t (id INT PRIMARY KEY ASC, a INT);",
			deparsed:     "create table t(id int primary key asc,a int)",
			expectedHash: "a360ddb202c0871558c0a3140a67e8a7a7a76e794a297214f7443e8739546408",
			expectedAST: &AST{
				Statements: []Statement{
					&CreateTable{
						Table:       &Table{Name: "t", IsTarget: true},
						Constraints: []TableConstraint{},
						ColumnsDef: []*ColumnDef{
							{
								Column: &Column{Name: "id"},
								Type:   TypeIntStr,
								Constraints: []ColumnConstraint{
									&ColumnConstraintPrimaryKey{Order: PrimaryKeyOrderAsc},
								},
							},
							{Column: &Column{Name: "a"}, Type: TypeIntStr, Constraints: []ColumnConstraint{}},
						},
					},
				},
			},
		},
		{
			name:         "create table primary key desc",
			stmt:         "CREATE TABLE t (id INT PRIMARY KEY DESC, a INT);",
			deparsed:     "create table t(id int primary key desc,a int)",
			expectedHash: "a360ddb202c0871558c0a3140a67e8a7a7a76e794a297214f7443e8739546408",
			expectedAST: &AST{
				Statements: []Statement{
					&CreateTable{
						Table:       &Table{Name: "t", IsTarget: true},
						Constraints: []TableConstraint{},
						ColumnsDef: []*ColumnDef{
							{
								Column: &Column{Name: "id"},
								Type:   TypeIntStr,
								Constraints: []ColumnConstraint{
									&ColumnConstraintPrimaryKey{Order: PrimaryKeyOrderDesc},
								},
							},
							{Column: &Column{Name: "a"}, Type: TypeIntStr, Constraints: []ColumnConstraint{}},
						},
					},
				},
			},
		},
		{
			name:         "create table primary key not null",
			stmt:         "CREATE TABLE t (id INT PRIMARY KEY CONSTRAINT nn NOT NULL, id2 INT NOT NULL);",
			deparsed:     "create table t(id int primary key constraint nn not null,id2 int not null)",
			expectedHash: "43a25e6519b90d5c1303898c3d3883360fcd4559fc0cbabd4015e5de9ab4d1cf",
			expectedAST: &AST{
				Statements: []Statement{
					&CreateTable{
						Table:       &Table{Name: "t", IsTarget: true},
						Constraints: []TableConstraint{},
						ColumnsDef: []*ColumnDef{
							{
								Column: &Column{Name: "id"},
								Type:   TypeIntStr,
								Constraints: []ColumnConstraint{
									&ColumnConstraintPrimaryKey{Order: PrimaryKeyOrderEmpty},
									&ColumnConstraintNotNull{
										Name: "nn",
									},
								},
							},
							{
								Column: &Column{Name: "id2"},
								Type:   TypeIntStr,
								Constraints: []ColumnConstraint{
									&ColumnConstraintNotNull{},
								},
							},
						},
					},
				},
			},
		},
		{
			name:         "create table unique",
			stmt:         "CREATE TABLE t (id INT UNIQUE, id2 INT CONSTRAINT un UNIQUE);",
			deparsed:     "create table t(id int unique,id2 int constraint un unique)",
			expectedHash: "43a25e6519b90d5c1303898c3d3883360fcd4559fc0cbabd4015e5de9ab4d1cf",
			expectedAST: &AST{
				Statements: []Statement{
					&CreateTable{
						Table:       &Table{Name: "t", IsTarget: true},
						Constraints: []TableConstraint{},
						ColumnsDef: []*ColumnDef{
							{
								Column: &Column{Name: "id"},
								Type:   TypeIntStr,
								Constraints: []ColumnConstraint{
									&ColumnConstraintUnique{},
								},
							},
							{
								Column: &Column{Name: "id2"},
								Type:   TypeIntStr,
								Constraints: []ColumnConstraint{
									&ColumnConstraintUnique{
										Name: "un",
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name:         "create table check",
			stmt:         "CREATE TABLE t (a INT CHECK(a > 2), id2 INT CONSTRAINT check_constraint CHECK(a > 2));",
			deparsed:     "create table t(a int check(a>2),id2 int constraint check_constraint check(a>2))",
			expectedHash: "0e93c25832cc90984a0157bdb71f7fa33172700a41c55cd9d896ff4c3d07d598",
			expectedAST: &AST{
				Statements: []Statement{
					&CreateTable{
						Table:       &Table{Name: "t", IsTarget: true},
						Constraints: []TableConstraint{},
						ColumnsDef: []*ColumnDef{
							{
								Column: &Column{Name: "a"},
								Type:   TypeIntStr,
								Constraints: []ColumnConstraint{
									&ColumnConstraintCheck{
										Expr: &CmpExpr{
											Operator: GreaterThanStr,
											Left:     &Column{Name: "a"},
											Right:    &Value{Type: IntValue, Value: []byte("2")},
										},
									},
								},
							},
							{
								Column: &Column{Name: "id2"},
								Type:   TypeIntStr,
								Constraints: []ColumnConstraint{
									&ColumnConstraintCheck{
										Name: "check_constraint",
										Expr: &CmpExpr{
											Operator: GreaterThanStr,
											Left:     &Column{Name: "a"},
											Right:    &Value{Type: IntValue, Value: []byte("2")},
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
			name:         "create table default",
			stmt:         "CREATE TABLE t (a INT CONSTRAINT default_constraint DEFAULT 0, b INT DEFAULT 1, c INT DEFAULT 0x1, d TEXT DEFAULT 'foo', e TEXT DEFAULT ('foo'), f INT DEFAULT +1);", // nolint
			deparsed:     "create table t(a int constraint default_constraint default 0,b int default 1,c int default 0x1,d text default 'foo',e text default ('foo'),f int default 1)",         // nolint
			expectedHash: "70a57145d62731d006bc23ede6126e3fe3f3f0a3954a87411edd2fb66ff59d7b",
			expectedAST: &AST{
				Statements: []Statement{
					&CreateTable{
						Table:       &Table{Name: "t", IsTarget: true},
						Constraints: []TableConstraint{},
						ColumnsDef: []*ColumnDef{
							{
								Column: &Column{Name: "a"},
								Type:   TypeIntStr,
								Constraints: []ColumnConstraint{
									&ColumnConstraintDefault{
										Name: "default_constraint",
										Expr: &Value{Type: IntValue, Value: []byte("0")},
									},
								},
							},
							{
								Column: &Column{Name: "b"},
								Type:   TypeIntStr,
								Constraints: []ColumnConstraint{
									&ColumnConstraintDefault{
										Expr: &Value{Type: IntValue, Value: []byte("1")},
									},
								},
							},
							{
								Column: &Column{Name: "c"},
								Type:   TypeIntStr,
								Constraints: []ColumnConstraint{
									&ColumnConstraintDefault{
										Expr: &Value{Type: HexNumValue, Value: []byte("0x1")},
									},
								},
							},
							{
								Column: &Column{Name: "d"},
								Type:   TypeTextStr,
								Constraints: []ColumnConstraint{
									&ColumnConstraintDefault{
										Expr: &Value{Type: StrValue, Value: []byte("foo")},
									},
								},
							},
							{
								Column: &Column{Name: "e"},
								Type:   TypeTextStr,
								Constraints: []ColumnConstraint{
									&ColumnConstraintDefault{
										Expr:        &Value{Type: StrValue, Value: []byte("foo")},
										Parenthesis: true,
									},
								},
							},
							{
								Column: &Column{Name: "f"},
								Type:   TypeIntStr,
								Constraints: []ColumnConstraint{
									&ColumnConstraintDefault{
										Expr: &Value{Type: IntValue, Value: []byte("1")},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name:         "create table generated",
			stmt:         "CREATE TABLE t (a INTEGER CONSTRAINT pk PRIMARY KEY, b INT, c TEXT, d INT CONSTRAINT gen GENERATED ALWAYS AS (a * abs(b)) VIRTUAL, e TEXT GENERATED ALWAYS AS (substr(c, b, b + 1)) STORED, f TEXT AS (substr(c, b, b + 1)));", // nolint
			deparsed:     "create table t(a integer constraint pk primary key autoincrement,b int,c text,d int constraint gen generated always as(a*abs(b)),e text generated always as(substr(c,b,b+1))stored,f text as(substr(c,b,b+1)))",                // nolint
			expectedHash: "09a0bb453d40af2c8cb23235d92658a73b7e4c0f3688bb8e81c32c48c2266be2",
			expectedAST: &AST{
				Statements: []Statement{
					&CreateTable{
						Table:       &Table{Name: "t", IsTarget: true},
						Constraints: []TableConstraint{},
						ColumnsDef: []*ColumnDef{
							{
								Column: &Column{Name: "a"},
								Type:   TypeIntegerStr,
								Constraints: []ColumnConstraint{
									&ColumnConstraintPrimaryKey{
										Name:          "pk",
										AutoIncrement: true,
									},
								},
							},
							{
								Column:      &Column{Name: "b"},
								Type:        TypeIntStr,
								Constraints: []ColumnConstraint{},
							},
							{
								Column:      &Column{Name: "c"},
								Type:        TypeTextStr,
								Constraints: []ColumnConstraint{},
							},
							{
								Column: &Column{Name: "d"},
								Type:   TypeIntStr,
								Constraints: []ColumnConstraint{
									&ColumnConstraintGenerated{
										Name:            "gen",
										GeneratedAlways: true,
										Expr: &BinaryExpr{
											Operator: MultStr,
											Left:     &Column{Name: "a"},
											Right: &FuncExpr{
												Name: "abs",
												Args: Exprs{
													&Column{Name: "b"},
												},
											},
										},
									},
								},
							},
							{
								Column: &Column{Name: "e"},
								Type:   TypeTextStr,
								Constraints: []ColumnConstraint{
									&ColumnConstraintGenerated{
										GeneratedAlways: true,
										IsStored:        true,
										Expr: &FuncExpr{
											Name: "substr",
											Args: Exprs{
												&Column{Name: "c"},
												&Column{Name: "b"},
												&BinaryExpr{
													Operator: PlusStr,
													Left:     &Column{Name: "b"},
													Right:    &Value{Type: IntValue, Value: []byte("1")},
												},
											},
										},
									},
								},
							},
							{
								Column: &Column{Name: "f"},
								Type:   TypeTextStr,
								Constraints: []ColumnConstraint{
									&ColumnConstraintGenerated{
										GeneratedAlways: false,
										IsStored:        false,
										Expr: &FuncExpr{
											Name: "substr",
											Args: Exprs{
												&Column{Name: "c"},
												&Column{Name: "b"},
												&BinaryExpr{
													Operator: PlusStr,
													Left:     &Column{Name: "b"},
													Right:    &Value{Type: IntValue, Value: []byte("1")},
												},
											},
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
			name:         "create table table constraints",
			stmt:         "CREATE TABLE t (id INT CONSTRAINT nm NOT NULL, id2 INT, CONSTRAINT pk PRIMARY KEY (id), CONSTRAINT un UNIQUE (id, id2), CONSTRAINT c CHECK(id > 0));", // nolint
			deparsed:     "create table t(id int constraint nm not null,id2 int,constraint pk primary key(id),constraint un unique(id,id2),constraint c check(id>0))",            // nolint
			expectedHash: "43a25e6519b90d5c1303898c3d3883360fcd4559fc0cbabd4015e5de9ab4d1cf",
			expectedAST: &AST{
				Statements: []Statement{
					&CreateTable{
						Table: &Table{Name: "t", IsTarget: true},
						ColumnsDef: []*ColumnDef{
							{
								Column: &Column{Name: "id"},
								Type:   TypeIntStr,
								Constraints: []ColumnConstraint{
									&ColumnConstraintNotNull{
										Name: "nm",
									},
								},
							},
							{
								Column:      &Column{Name: "id2"},
								Type:        TypeIntStr,
								Constraints: []ColumnConstraint{},
							},
						},
						Constraints: []TableConstraint{
							&TableConstraintPrimaryKey{
								Name: "pk",
								Columns: IndexedColumnList{
									&IndexedColumn{
										Column: &Column{Name: "id"},
									},
								},
							},
							&TableConstraintUnique{
								Name: "un",
								Columns: ColumnList{
									&Column{Name: "id"},
									&Column{Name: "id2"},
								},
							},
							&TableConstraintCheck{
								Name: "c",
								Expr: &CmpExpr{
									Operator: GreaterThanStr,
									Left:     &Column{Name: "id"},
									Right:    &Value{Type: IntValue, Value: []byte("0")},
								},
							},
						},
					},
				},
			},
		},
		{
			name:         "create table replace table constraint",
			stmt:         "CREATE TABLE t(x INTEGER, PRIMARY KEY (x));",
			deparsed:     "create table t(x integer primary key autoincrement)",
			expectedHash: "858688370cc6ddf501ebbfe878877e83edeaaf247d1e12faff8bb77ce904e935",
			expectedAST: &AST{
				Statements: []Statement{
					&CreateTable{
						Table: &Table{Name: "t", IsTarget: true},
						ColumnsDef: []*ColumnDef{
							{
								Column: &Column{Name: "x"},
								Type:   TypeIntegerStr,
								Constraints: []ColumnConstraint{
									&ColumnConstraintPrimaryKey{
										Name:          "",
										AutoIncrement: true,
									},
								},
							},
						},
						Constraints: []TableConstraint{},
					},
				},
			},
		},
		{
			name:         "create table replace table constraint with name",
			stmt:         "CREATE TABLE t(x INTEGER, CONSTRAINT pk PRIMARY KEY (x));",
			deparsed:     "create table t(x integer constraint pk primary key autoincrement)",
			expectedHash: "858688370cc6ddf501ebbfe878877e83edeaaf247d1e12faff8bb77ce904e935",
			expectedAST: &AST{
				Statements: []Statement{
					&CreateTable{
						Table: &Table{Name: "t", IsTarget: true},
						ColumnsDef: []*ColumnDef{
							{
								Column: &Column{Name: "x"},
								Type:   TypeIntegerStr,
								Constraints: []ColumnConstraint{
									&ColumnConstraintPrimaryKey{
										Name:          "pk",
										AutoIncrement: true,
									},
								},
							},
						},
						Constraints: []TableConstraint{},
					},
				},
			},
		},
		{
			name:         "create table replace table constraint middle",
			stmt:         "CREATE TABLE t(x INTEGER NOT NULL, b INT, check(b>0), PRIMARY KEY (x DESC), UNIQUE (b));",
			deparsed:     "create table t(x integer not null primary key desc,b int,check(b>0),unique(b))",
			expectedHash: "783f0ed7cc77247b44f69325d3c865b05ebdbc9c8087d367d685ea63af87fdbd",
			expectedAST: &AST{
				Statements: []Statement{
					&CreateTable{
						Table: &Table{Name: "t", IsTarget: true},
						ColumnsDef: []*ColumnDef{
							{
								Column: &Column{Name: "x"},
								Type:   TypeIntegerStr,
								Constraints: []ColumnConstraint{
									&ColumnConstraintNotNull{},
									&ColumnConstraintPrimaryKey{
										Name:          "",
										AutoIncrement: false,
										Order:         "desc",
									},
								},
							},
							{
								Column:      &Column{Name: "b"},
								Type:        TypeIntStr,
								Constraints: []ColumnConstraint{},
							},
						},
						Constraints: []TableConstraint{
							&TableConstraintCheck{
								Expr: &CmpExpr{
									Operator: GreaterThanStr,
									Left:     &Column{Name: "b"},
									Right:    &Value{Type: IntValue, Value: []byte("0")},
								},
							},
							&TableConstraintUnique{
								Columns: ColumnList{
									&Column{Name: "b"},
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
				require.Len(t, ast.Errors, 0)
				require.Equal(t, tc.expectedHash, ast.Statements[0].(*CreateTable).StructureHash())
				require.Equal(t, tc.expectedAST, ast)
				require.Equal(t, tc.deparsed, ast.String())

				// test all CREATE statements against SQLite3
				db, err := sql.Open("sqlite3", "file::"+uuid.NewString()+":?mode=memory&cache=shared&_foreign_keys=on")
				require.NoError(t, err)

				_, err = db.Exec(tc.stmt)
				require.NoError(t, err)

				_, err = db.Exec(fmt.Sprintf("DROP TABLE %s", ast.Statements[0].(*CreateTable).Table.Name))
				require.NoError(t, err)

				// test AST SQL generation against SQLite3
				_, err = db.Exec(ast.String())
				require.NoError(t, err)
				require.NoError(t, db.Close())
			}
		}(tc))
	}
}

func TestCreateTableStrict(t *testing.T) {
	t.Parallel()
	ast, err := Parse("create table t (a int);")
	require.NoError(t, err)

	ast.Statements[0].(*CreateTable).StrictMode = true

	require.Equal(t, "create table t(a int)strict", ast.String())
}

func TestCreateTableAutoIncrementRules(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name     string
		stmt     string
		deparsed string
	}

	// - CREATE TABLE t(a INTEGER PRIMARY KEY) -> automatically injects AUTOINCREMENT
	// - CREATE TABLE t(a INTEGER PRIMARY KEY DESC) -> as is, since isn't an alias.
	// - CREATE TABLE t(a XXXX PRIMARY KEY) where XXXX isn't INTEGER, as is.
	// - CREATE TABLE t(a INTEGER, PRIMARY KEY(a ASC)) -> automatically transformed to first bullet
	// - CREATE TABLE t(a INTEGER, PRIMARY KEY(a DESC)) -> automatically transformed to second bullet.

	tests := []testCase{
		{
			"integer primary key forces autoincrement",
			"CREATE TABLE t (a INTEGER PRIMARY KEY)",
			"create table t(a integer primary key autoincrement)",
		},
		{
			"integer primary key desc",
			"CREATE TABLE t (a INTEGER PRIMARY KEY DESC)",
			"create table t(a integer primary key desc)",
		},
		{
			"non integer primary key",
			"CREATE TABLE t (a INT PRIMARY KEY)",
			"create table t(a int primary key)",
		},
		{
			"integer table primary key forces autoincrement",
			"CREATE TABLE t (a INTEGER, PRIMARY KEY(a ASC))",
			"create table t(a integer primary key asc autoincrement)",
		},
		{
			"integer table primary key desc",
			"CREATE TABLE t (a INTEGER, PRIMARY KEY(a DESC))",
			"create table t(a integer primary key desc)",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(tc testCase) func(t *testing.T) {
			return func(t *testing.T) {
				t.Parallel()
				ast, err := Parse(tc.stmt)
				require.NoError(t, err)
				require.Len(t, ast.Errors, 0)
				require.Equal(t, tc.deparsed, ast.String())

				// test all CREATE statements against SQLite3
				db, err := sql.Open("sqlite3", "file::"+uuid.NewString()+":?mode=memory&cache=shared&_foreign_keys=on")
				require.NoError(t, err)

				_, err = db.Exec(tc.stmt)
				require.NoError(t, err)

				_, err = db.Exec(fmt.Sprintf("DROP TABLE %s", ast.Statements[0].(*CreateTable).Table.Name))
				require.NoError(t, err)

				// test AST SQL generation against SQLite3
				_, err = db.Exec(ast.String())
				require.NoError(t, err)
				require.NoError(t, db.Close())
			}
		}(tc))
	}
}

func TestInsert(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name        string
		stmt        string
		deparsed    string
		expectedAST *AST
		expectedErr error
	}

	tests := []testCase{
		{
			name:     "insert simple",
			stmt:     "INSERT INTO t (a, b) VALUES (1, 2), (3, 4);",
			deparsed: "insert into t(a,b)values(1,2),(3,4)",
			expectedAST: &AST{
				Statements: []Statement{
					&Insert{
						Table: &Table{Name: "t", IsTarget: true},
						Columns: ColumnList{
							&Column{Name: "a"},
							&Column{Name: "b"},
						},
						Rows: []Exprs{
							{
								&Value{Type: IntValue, Value: []byte("1")},
								&Value{Type: IntValue, Value: []byte("2")},
							},
							{
								&Value{Type: IntValue, Value: []byte("3")},
								&Value{Type: IntValue, Value: []byte("4")},
							},
						},
					},
				},
			},
		},
		{
			name:     "insert skip columns",
			stmt:     "INSERT INTO t VALUES (1, 2), (3, 4);",
			deparsed: "insert into t values(1,2),(3,4)",
			expectedAST: &AST{
				Statements: []Statement{
					&Insert{
						Table:   &Table{Name: "t", IsTarget: true},
						Columns: ColumnList{},
						Rows: []Exprs{
							{
								&Value{Type: IntValue, Value: []byte("1")},
								&Value{Type: IntValue, Value: []byte("2")},
							},
							{
								&Value{Type: IntValue, Value: []byte("3")},
								&Value{Type: IntValue, Value: []byte("4")},
							},
						},
					},
				},
			},
		},
		{
			name:     "insert default values",
			stmt:     "INSERT INTO t DEFAULT VALUES;",
			deparsed: "insert into t default values",
			expectedAST: &AST{
				Statements: []Statement{
					&Insert{
						Table:         &Table{Name: "t", IsTarget: true},
						Columns:       ColumnList{},
						Rows:          []Exprs{},
						DefaultValues: true,
					},
				},
			},
		},
		{
			name:     "upsert do nothing",
			stmt:     "INSERT INTO t (id) VALUES (1) ON CONFLICT DO NOTHING;",
			deparsed: "insert into t(id)values(1)on conflict do nothing",
			expectedAST: &AST{
				Statements: []Statement{
					&Insert{
						Table: &Table{Name: "t", IsTarget: true},
						Columns: ColumnList{
							&Column{Name: "id"},
						},
						Rows: []Exprs{
							{
								&Value{Type: IntValue, Value: []byte("1")},
							},
						},
						DefaultValues: false,
						Upsert: Upsert{
							&OnConflictClause{},
						},
					},
				},
			},
		},
		{
			name:     "upsert do nothing with target",
			stmt:     "INSERT INTO t (id) VALUES (1) ON CONFLICT (id) DO NOTHING;",
			deparsed: "insert into t(id)values(1)on conflict(id)do nothing",
			expectedAST: &AST{
				Statements: []Statement{
					&Insert{
						Table: &Table{Name: "t", IsTarget: true},
						Columns: ColumnList{
							&Column{Name: "id"},
						},
						Rows: []Exprs{
							{
								&Value{Type: IntValue, Value: []byte("1")},
							},
						},
						DefaultValues: false,
						Upsert: Upsert{
							&OnConflictClause{
								Target: &OnConflictTarget{
									Columns: []*Column{
										{Name: "id"},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name:     "upsert do update with target",
			stmt:     "INSERT INTO t (id, count) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET count = count + 1;",
			deparsed: "insert into t(id,count)values(1,1)on conflict(id)do update set count=count+1",
			expectedAST: &AST{
				Statements: []Statement{
					&Insert{
						Table: &Table{Name: "t", IsTarget: true},
						Columns: ColumnList{
							&Column{Name: "id"},
							&Column{Name: "count"},
						},
						Rows: []Exprs{
							{
								&Value{Type: IntValue, Value: []byte("1")},
								&Value{Type: IntValue, Value: []byte("1")},
							},
						},
						DefaultValues: false,
						Upsert: Upsert{
							&OnConflictClause{
								Target: &OnConflictTarget{
									Columns: []*Column{
										{Name: "id"},
									},
								},
								DoUpdate: &OnConflictUpdate{
									Exprs: []*UpdateExpr{
										{
											Column: &Column{Name: "count"},
											Expr: &BinaryExpr{
												Operator: PlusStr,
												Left:     &Column{Name: "count"},
												Right:    &Value{Type: IntValue, Value: []byte("1")},
											},
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
			name:     "upsert do update with target excluded",
			stmt:     "INSERT INTO phonebook(name,phonenumber) VALUES('Alice','704-555-1212') ON CONFLICT(name) DO UPDATE SET phonenumber=excluded.phonenumber;", // nolint
			deparsed: "insert into phonebook(name,phonenumber)values('Alice','704-555-1212')on conflict(name)do update set phonenumber=excluded.phonenumber",     // nolint
			expectedAST: &AST{
				Statements: []Statement{
					&Insert{
						Table: &Table{Name: "phonebook", IsTarget: true},
						Columns: ColumnList{
							&Column{Name: "name"},
							&Column{Name: "phonenumber"},
						},
						Rows: []Exprs{
							{
								&Value{Type: StrValue, Value: []byte("Alice")},
								&Value{Type: StrValue, Value: []byte("704-555-1212")},
							},
						},
						DefaultValues: false,
						Upsert: Upsert{
							&OnConflictClause{
								Target: &OnConflictTarget{
									Columns: []*Column{
										{Name: "name"},
									},
								},
								DoUpdate: &OnConflictUpdate{
									Exprs: []*UpdateExpr{
										{
											Column: &Column{Name: "phonenumber"},
											Expr: &Column{
												TableRef: &Table{Name: "excluded"},
												Name:     "phonenumber",
											},
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
			name:     "upsert do update with target excluded with where",
			stmt:     "INSERT INTO phonebook(name,phonenumber) VALUES('Alice','704-555-1212') ON CONFLICT(name) DO UPDATE SET phonenumber=excluded.phonenumber WHERE excluded.phonenumber != '';", // nolint
			deparsed: "insert into phonebook(name,phonenumber)values('Alice','704-555-1212')on conflict(name)do update set phonenumber=excluded.phonenumber where excluded.phonenumber!=''",       // nolint
			expectedAST: &AST{
				Statements: []Statement{
					&Insert{
						Table: &Table{Name: "phonebook", IsTarget: true},
						Columns: ColumnList{
							&Column{Name: "name"},
							&Column{Name: "phonenumber"},
						},
						Rows: []Exprs{
							{
								&Value{Type: StrValue, Value: []byte("Alice")},
								&Value{Type: StrValue, Value: []byte("704-555-1212")},
							},
						},
						DefaultValues: false,
						Upsert: Upsert{
							&OnConflictClause{
								Target: &OnConflictTarget{
									Columns: []*Column{
										{Name: "name"},
									},
								},
								DoUpdate: &OnConflictUpdate{
									Exprs: []*UpdateExpr{
										{
											Column: &Column{Name: "phonenumber"},
											Expr: &Column{
												TableRef: &Table{Name: "excluded"},
												Name:     "phonenumber",
											},
										},
									},
									Where: &Where{
										Type: WhereStr,
										Expr: &CmpExpr{
											Operator: NotEqualStr,
											Left: &Column{
												TableRef: &Table{Name: "excluded"},
												Name:     "phonenumber",
											},
											Right: &Value{Type: StrValue, Value: []byte("")},
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
			name:     "upsert multiple clauses",
			stmt:     "INSERT INTO t (id) VALUES (1) ON CONFLICT (id) DO NOTHING ON CONFLICT DO NOTHING;",
			deparsed: "insert into t(id)values(1)on conflict(id)do nothing on conflict do nothing",
			expectedAST: &AST{
				Statements: []Statement{
					&Insert{
						Table: &Table{Name: "t", IsTarget: true},
						Columns: ColumnList{
							&Column{Name: "id"},
						},
						Rows: []Exprs{
							{
								&Value{Type: IntValue, Value: []byte("1")},
							},
						},
						DefaultValues: false,
						Upsert: Upsert{
							&OnConflictClause{
								Target: &OnConflictTarget{
									Columns: ColumnList{
										{
											Name: "id",
										},
									},
								},
							},
							&OnConflictClause{},
						},
					},
				},
			},
		},
		{
			name:        "upsert multiple clauses missing target",
			stmt:        "INSERT INTO t (id) VALUES (1) ON CONFLICT DO NOTHING ON CONFLICT DO NOTHING;",
			deparsed:    "insert into t(id)values(1)on conflict do nothing on conflict do nothing",
			expectedAST: nil,
			expectedErr: &ErrUpsertMissingTarget{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(tc testCase) func(t *testing.T) {
			return func(t *testing.T) {
				t.Parallel()
				ast, err := Parse(tc.stmt)
				if tc.expectedErr == nil {
					require.NoError(t, err)
					require.Len(t, ast.Errors, 0)
					require.Equal(t, tc.expectedAST, ast)
					require.Equal(t, tc.deparsed, ast.String())
				} else {
					require.ErrorAs(t, ast.Errors[0], &tc.expectedErr)
				}
			}
		}(tc))
	}
}

func TestDelete(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name        string
		stmt        string
		deparsed    string
		expectedAST *AST
	}

	tests := []testCase{
		{
			name:     "delete simple",
			stmt:     "DELETE FROM t;",
			deparsed: "delete from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Delete{
						Table: &Table{Name: "t", IsTarget: true},
					},
				},
			},
		},
		{
			name:     "delete with where",
			stmt:     "DELETE FROM t WHERE a = 1;",
			deparsed: "delete from t where a=1",
			expectedAST: &AST{
				Statements: []Statement{
					&Delete{
						Table: &Table{Name: "t", IsTarget: true},
						Where: &Where{
							Type: WhereStr,
							Expr: &CmpExpr{
								Operator: EqualStr,
								Left:     &Column{Name: "a"},
								Right:    &Value{Type: IntValue, Value: []byte("1")},
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
				require.Len(t, ast.Errors, 0)
				require.Equal(t, tc.expectedAST, ast)
				require.Equal(t, tc.deparsed, ast.String())
			}
		}(tc))
	}
}

func TestUpdate(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name        string
		stmt        string
		deparsed    string
		expectedAST *AST
		expectedErr error
	}

	tests := []testCase{
		{
			name:     "update simple",
			stmt:     "update t set a = 1, b = 2;",
			deparsed: "update t set a=1,b=2",
			expectedAST: &AST{
				Statements: []Statement{
					&Update{
						Table: &Table{Name: "t", IsTarget: true},
						Exprs: []*UpdateExpr{
							{Column: &Column{Name: "a"}, Expr: &Value{Type: IntValue, Value: []byte("1")}},
							{Column: &Column{Name: "b"}, Expr: &Value{Type: IntValue, Value: []byte("2")}},
						},
					},
				},
			},
		},
		{
			name:     "update parenthesis",
			stmt:     "update t set (a, b) = (1, 2);",
			deparsed: "update t set a=1,b=2",
			expectedAST: &AST{
				Statements: []Statement{
					&Update{
						Table: &Table{Name: "t", IsTarget: true},
						Exprs: []*UpdateExpr{
							{Column: &Column{Name: "a"}, Expr: &Value{Type: IntValue, Value: []byte("1")}},
							{Column: &Column{Name: "b"}, Expr: &Value{Type: IntValue, Value: []byte("2")}},
						},
					},
				},
			},
		},
		{
			name:        "update wrong number of exprs",
			stmt:        "update t set (a, b) = (1);",
			deparsed:    "",
			expectedAST: nil,
			expectedErr: &ErrUpdateColumnsAndValuesDiffer{ColumnsCount: 2, ValuesCount: 1},
		},
		{
			name:     "update with where",
			stmt:     "update t set a = 1, b = 2 where a = 3;",
			deparsed: "update t set a=1,b=2 where a=3",
			expectedAST: &AST{
				Statements: []Statement{
					&Update{
						Table: &Table{Name: "t", IsTarget: true},
						Exprs: []*UpdateExpr{
							{Column: &Column{Name: "a"}, Expr: &Value{Type: IntValue, Value: []byte("1")}},
							{Column: &Column{Name: "b"}, Expr: &Value{Type: IntValue, Value: []byte("2")}},
						},
						Where: &Where{
							Type: WhereStr,
							Expr: &CmpExpr{
								Operator: EqualStr,
								Left:     &Column{Name: "a"},
								Right:    &Value{Type: IntValue, Value: []byte("3")},
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
				if tc.expectedErr == nil {
					require.NoError(t, err)
					require.Len(t, ast.Errors, 0)
					require.Equal(t, tc.expectedAST, ast)
					require.Equal(t, tc.deparsed, ast.String())
				} else {
					require.ErrorAs(t, ast.Errors[0], &tc.expectedErr)
				}
			}
		}(tc))
	}
}

func TestGrant(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name        string
		stmt        string
		deparsed    string
		expectedAST *AST
		expectedErr error
	}

	tests := []testCase{
		{
			name:     "grant",
			stmt:     "GRANT INSERT, UPDATE, DELETE on t TO 'a', 'b'",
			deparsed: "grant delete,insert,update on t to 'a', 'b'",
			expectedAST: &AST{
				Statements: []Statement{
					&Grant{
						Table: &Table{Name: "t", IsTarget: true},
						Privileges: Privileges{
							"insert": struct{}{},
							"update": struct{}{},
							"delete": struct{}{},
						},
						Roles: []string{"a", "b"},
					},
				},
			},
		},
		{
			name:        "grant repeated",
			stmt:        "GRANT INSERT, DELETE, DELETE on t TO 'a', 'b'",
			deparsed:    "",
			expectedAST: nil,
			expectedErr: &ErrGrantRepeatedPrivilege{Privilege: "delete"},
		},
		{
			name:     "revoke",
			stmt:     "REVOKE INSERT, UPDATE, DELETE ON t FROM 'a', 'b'",
			deparsed: "revoke delete,insert,update on t from 'a', 'b'",
			expectedAST: &AST{
				Statements: []Statement{
					&Revoke{
						Table: &Table{Name: "t", IsTarget: true},
						Privileges: Privileges{
							"insert": struct{}{},
							"update": struct{}{},
							"delete": struct{}{},
						},
						Roles: []string{"a", "b"},
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
				if tc.expectedErr == nil {
					require.NoError(t, err)
					require.Len(t, ast.Errors, 0)
					require.Equal(t, tc.expectedAST, ast)
					require.Equal(t, tc.deparsed, ast.String())
				} else {
					require.ErrorAs(t, ast.Errors[0], &tc.expectedErr)
				}
			}
		}(tc))
	}
}

func TestMultipleStatements(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name           string
		stmt           string
		deparsed       string
		expectedAST    *AST
		expectedErrMsg string
	}

	tests := []testCase{
		{
			name:           "select multiple error",
			stmt:           "select * FROM t; select * FROM t;",
			deparsed:       "",
			expectedAST:    nil,
			expectedErrMsg: "syntax error",
		},
		{
			name:           "select and insert",
			stmt:           "select * FROM t; insert into table t (a) values (1);",
			deparsed:       "",
			expectedAST:    nil,
			expectedErrMsg: "syntax error",
		},
		{
			name:           "create multiple error",
			stmt:           "create table t (a int); create table t (a int);",
			deparsed:       "",
			expectedAST:    nil,
			expectedErrMsg: "syntax error",
		},
		{
			name: "multiple statements",
			stmt: `
			INSERT INTO t (a, b) VALUES (1,2),(3,4);
			delete from t;
			update t set a = 1, b = 2;
			GRANT INSERT, UPDATE, DELETE on t TO 'a', 'b'
			REVOKE INSERT, UPDATE, DELETE ON t FROM 'a', 'b'
			`,
			deparsed: "",
			expectedAST: &AST{
				Statements: []Statement{
					&Insert{
						Table: &Table{Name: "t", IsTarget: true},
						Columns: ColumnList{
							&Column{Name: "a"},
							&Column{Name: "b"},
						},
						Rows: []Exprs{
							{
								&Value{Type: IntValue, Value: []byte("1")},
								&Value{Type: IntValue, Value: []byte("2")},
							},
							{
								&Value{Type: IntValue, Value: []byte("3")},
								&Value{Type: IntValue, Value: []byte("4")},
							},
						},
					},
					&Delete{
						Table: &Table{Name: "t", IsTarget: true},
					},
					&Update{
						Table: &Table{Name: "t", IsTarget: true},
						Exprs: []*UpdateExpr{
							{Column: &Column{Name: "a"}, Expr: &Value{Type: IntValue, Value: []byte("1")}},
							{Column: &Column{Name: "b"}, Expr: &Value{Type: IntValue, Value: []byte("2")}},
						},
					},
					&Grant{
						Table: &Table{Name: "t", IsTarget: true},
						Privileges: Privileges{
							"insert": struct{}{},
							"update": struct{}{},
							"delete": struct{}{},
						},
						Roles: []string{"a", "b"},
					},
					&Revoke{
						Table: &Table{Name: "t", IsTarget: true},
						Privileges: Privileges{
							"insert": struct{}{},
							"update": struct{}{},
							"delete": struct{}{},
						},
						Roles: []string{"a", "b"},
					},
				},
			},
			expectedErrMsg: "syntax error",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(tc testCase) func(t *testing.T) {
			return func(t *testing.T) {
				t.Parallel()
				ast, err := Parse(tc.stmt)
				if tc.expectedErrMsg == "" {
					require.NoError(t, err)
					require.Len(t, ast.Errors, 0)
					require.Equal(t, tc.expectedAST, ast)
					require.Equal(t, tc.deparsed, ast.String())
				} else {
					require.Contains(t, err.Error(), tc.expectedErrMsg)
				}
			}
		}(tc))
	}
}

func TestAddWhere(t *testing.T) {
	t.Parallel()

	where := &Where{
		Type: WhereStr,
		Expr: &CmpExpr{
			Operator: EqualStr,
			Left:     &Column{Name: "b"},
			Right:    &Value{Type: IntValue, Value: []byte("2")},
		},
	}

	{
		ast, err := Parse("update t SET a = 1")
		require.NoError(t, err)

		updateStmt := ast.Statements[0].(*Update)
		updateStmt.AddWhereClause(where)
		require.Equal(t, "update t set a=1 where b=2", ast.String())
	}

	{
		ast, err := Parse("update t SET a = 1 WHERE a = 2")
		require.NoError(t, err)

		updateStmt := ast.Statements[0].(*Update)
		updateStmt.AddWhereClause(where)
		require.Equal(t, "update t set a=1 where a=2 and b=2", ast.String())
	}
	{
		ast, err := Parse("delete from t")
		require.NoError(t, err)

		deleteStmt := ast.Statements[0].(*Delete)
		deleteStmt.AddWhereClause(where)

		require.Equal(t, "delete from t where b=2", ast.String())
	}

	{
		ast, err := Parse("delete from t WHERE a = 2")
		require.NoError(t, err)

		deleteStmt := ast.Statements[0].(*Delete)
		deleteStmt.AddWhereClause(where)
		require.Equal(t, "delete from t where a=2 and b=2", ast.String())
	}
}

func TestKeywordsNotAllowed(t *testing.T) {
	t.Parallel()

	for keyword := range keywordsNotAllowed {
		ast, err := Parse(fmt.Sprintf("select %s from t", keyword))
		require.Error(t, err)
		require.Len(t, ast.Errors, 1)

		var e *ErrKeywordIsNotAllowed
		require.ErrorAs(t, ast.Errors[0], &e)
		if errors.As(ast.Errors[0], &e) {
			require.Equal(t, keyword, e.Keyword)
		}
		require.ErrorAs(t, err, &e)
	}
}

func TestLimits(t *testing.T) {
	t.Parallel()

	t.Run("max text length", func(t *testing.T) {
		t.Parallel()
		text := ""
		for i := 0; i <= MaxTextLength; i++ {
			text = text + "a"
		}

		ast, err := Parse(fmt.Sprintf("insert into t (a) values ('%s')", text))
		require.Error(t, err)
		require.Len(t, ast.Errors, 1)

		var e *ErrTextTooLong
		require.ErrorAs(t, ast.Errors[0], &e)
		if errors.As(ast.Errors[0], &e) {
			require.Equal(t, len(text), e.Length)
			require.Equal(t, MaxBlobLength, e.MaxAllowed)
		}
		require.ErrorAs(t, err, &e)
	})

	t.Run("max blob length", func(t *testing.T) {
		t.Parallel()
		blob := ""
		for i := 0; i <= MaxBlobLength; i++ {
			blob = blob + "f"
		}

		ast, err := Parse(fmt.Sprintf("insert into t (a) values (x'%s')", blob))
		require.Error(t, err)
		require.Len(t, ast.Errors, 1)

		var e *ErrBlobTooBig
		require.ErrorAs(t, ast.Errors[0], &e)
		if errors.As(ast.Errors[0], &e) {
			require.Equal(t, len(blob), e.Length)
			require.Equal(t, MaxBlobLength, e.MaxAllowed)
		}
		require.ErrorAs(t, err, &e)
	})

	t.Run("max columns allowed", func(t *testing.T) {
		t.Parallel()

		// generate a list (a, ..., z, aa, ab, ...) of MaxAllowedColumns + 1 columns
		columnsDef := []string{}
		for i := 1; i <= MaxAllowedColumns; i++ {
			column, j := []byte{}, i
			for j > 0 {
				column = append(column, byte('a'+j%('z'-'a'+1)))
				j = j / ('z' - 'a' + 1)
			}
			columnsDef = append(columnsDef, string(column)+" INT")
		}
		columnsDef = append([]string{"a INT"}, columnsDef...)

		ast, err := Parse(fmt.Sprintf("create table t (%s);", strings.Join(columnsDef, ", ")))
		require.Error(t, err)
		require.Len(t, ast.Errors, 1)

		var e *ErrTooManyColumns
		require.ErrorAs(t, ast.Errors[0], &e)
		if errors.As(ast.Errors[0], &e) {
			require.Equal(t, len(columnsDef), e.ColumnCount)
			require.Equal(t, MaxAllowedColumns, e.MaxAllowed)
		}
		require.ErrorAs(t, err, &e)
	})
}

func TestDisallowSubqueriesOnStatements(t *testing.T) {
	t.Parallel()
	t.Run("insert", func(t *testing.T) {
		ast, err := Parse("insert into t (a) VALUES ((select 1 FROM t limit 1))")
		require.Error(t, err)
		require.Len(t, ast.Errors, 1)

		var e *ErrStatementContainsSubquery
		require.ErrorAs(t, ast.Errors[0], &e)
		if errors.As(ast.Errors[0], &e) {
			require.Equal(t, "insert", e.StatementKind)
		}
		require.ErrorAs(t, err, &e)
		require.Equal(t, true, containsSubquery(ast))
	})

	t.Run("update update expr", func(t *testing.T) {
		ast, err := Parse("update t set a = (select 1 FROM t limit 1)")
		require.Error(t, err)
		require.Len(t, ast.Errors, 1)

		var e *ErrStatementContainsSubquery
		require.ErrorAs(t, ast.Errors[0], &e)
		if errors.As(ast.Errors[0], &e) {
			require.Equal(t, "update", e.StatementKind)
		}
		require.ErrorAs(t, err, &e)
		require.Equal(t, true, containsSubquery(ast))
	})

	t.Run("update where", func(t *testing.T) {
		ast, err := Parse("update foo set a=1 where a=(select a from bar limit 1) and b=1")
		require.Error(t, err)
		require.Len(t, ast.Errors, 1)

		var e *ErrStatementContainsSubquery
		require.ErrorAs(t, ast.Errors[0], &e)
		if errors.As(ast.Errors[0], &e) {
			require.Equal(t, "where", e.StatementKind)
		}
		require.ErrorAs(t, err, &e)
		require.Equal(t, true, containsSubquery(ast))
	})

	t.Run("delete", func(t *testing.T) {
		ast, err := Parse("delete from t where a or (select 1 FROM t limit 1)")
		require.Error(t, err)
		require.Len(t, ast.Errors, 1)

		var e *ErrStatementContainsSubquery
		require.ErrorAs(t, ast.Errors[0], &e)
		if errors.As(ast.Errors[0], &e) {
			require.Equal(t, "delete", e.StatementKind)
		}
		require.ErrorAs(t, err, &e)
		require.Equal(t, true, containsSubquery(ast))
	})

	t.Run("upsert", func(t *testing.T) {
		ast, err := Parse("INSERT INTO t (id, count) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET count = count + 1 WHERE (SELECT 1 FROM t2);") // nolint
		require.Error(t, err)
		require.Len(t, ast.Errors, 1)

		var e *ErrStatementContainsSubquery
		require.ErrorAs(t, ast.Errors[0], &e)
		if errors.As(ast.Errors[0], &e) {
			require.Equal(t, "where", e.StatementKind)
		}
		require.ErrorAs(t, err, &e)
		require.Equal(t, true, containsSubquery(ast))
	})
}

func TestMultipleErrors(t *testing.T) {
	t.Parallel()
	ast, err := Parse("UPDATE t SET a = (select 1 from t2), b = unknown()")
	require.Error(t, err)
	require.Len(t, ast.Errors, 1)

	var e1 *ErrStatementContainsSubquery
	var e2 *ErrNoSuchFunction
	require.ErrorAs(t, ast.Errors[0], &e1)
	require.ErrorAs(t, ast.Errors[0], &e2)
	if errors.As(ast.Errors[0], &e1) {
		require.Equal(t, "update", e1.StatementKind)
	}
	if errors.As(ast.Errors[0], &e2) {
		require.Equal(t, "unknown", e2.FunctionName)
	}

	require.ErrorAs(t, err, &e1)
}

func TestParallel(t *testing.T) {
	parallelism := 200
	numIters := 500

	type testCase struct {
		stmt     string
		deparsed string
	}

	tests := []testCase{
		{
			stmt:     "SELECT true FROM t",
			deparsed: "select true from t",
		},
		{
			stmt:     "SELECT FALSE FROM t",
			deparsed: "select false from t",
		},
		{
			stmt:     "SELECT 'anything between single quotes is a string' FROM t",
			deparsed: "select 'anything between single quotes is a string' from t",
		},
		{
			stmt:     "SELECT 'bruno''s car' FROM t",
			deparsed: "select 'bruno''s car' from t",
		},
		{
			stmt:     "SELECT 12 FROM t",
			deparsed: "select 12 from t",
		},
		{
			stmt:     "SELECT -12 FROM t",
			deparsed: "select -12 from t",
		},
		{
			stmt:     "SELECT 0xAF12 FROM t",
			deparsed: "select 0xAF12 from t",
		},
		{
			stmt:     "SELECT x'AF12' FROM t",
			deparsed: "select X'AF12' from t",
		},
		{
			stmt:     "SELECT X'AF12' FROM t",
			deparsed: "select X'AF12' from t",
		},
		{
			stmt:     "SELECT null FROM t",
			deparsed: "select null from t",
		},
		{
			stmt:     "SELECT NULL FROM t",
			deparsed: "select null from t",
		},
		{
			stmt:     "SELECT thisisacolumn FROM t",
			deparsed: "select thisisacolumn from t",
		},
		{
			stmt:     "SELECT this_is_a_column3208ADKJHKDS_ FROM t",
			deparsed: "select this_is_a_column3208ADKJHKDS_ from t",
		},
		{
			stmt:     "SELECT _also_column FROM t",
			deparsed: "select _also_column from t",
		},
		{
			stmt:     "SELECT -a FROM t",
			deparsed: "select -a from t",
		},
		{
			stmt:     "SELECT a = 2 FROM t",
			deparsed: "select a=2 from t",
		},
		{
			stmt:     "SELECT a != 2 FROM t",
			deparsed: "select a!=2 from t",
		},
		{
			stmt:     "SELECT a < 2 FROM t",
			deparsed: "select a<2 from t",
		},
		{
			stmt:     "SELECT a >= 2 FROM t",
			deparsed: "select a>=2 from t",
		},
		{
			stmt:     "SELECT a <= 2 FROM t",
			deparsed: "select a<=2 from t",
		},
		{
			stmt:     "SELECT a glob 'a' FROM t",
			deparsed: "select a glob 'a' from t",
		},
		{
			stmt:     "SELECT a match 'a' FROM t",
			deparsed: "select a match 'a' from t",
		},
		{
			stmt:     "SELECT a like 'a' FROM t",
			deparsed: "select a like 'a' from t",
		},
		{
			stmt:     "SELECT a not like '%a\\%%' escape '\\' FROM t",
			deparsed: "select a not like '%a\\%%' escape '\\' from t",
		},
		{
			stmt:     "SELECT a and b FROM t",
			deparsed: "select a and b from t",
		},
		{
			stmt:     "SELECT a or b FROM t",
			deparsed: "select a or b from t",
		},
		{
			stmt:     "SELECT a is b FROM t",
			deparsed: "select a is b from t",
		},
		{
			stmt:     "SELECT a is not b FROM t",
			deparsed: "select a is not b from t",
		},
		{
			stmt:     "SELECT a isnull FROM t",
			deparsed: "select a isnull from t",
		},
		{
			stmt:     "SELECT a not null FROM t",
			deparsed: "select a notnull from t",
		},
		{
			stmt:     "SELECT CAST (1 AS TEXT) FROM t",
			deparsed: "select cast(1 as text)from t",
		},
		{
			stmt:     "SELECT CAST (a AS none) FROM t",
			deparsed: "select cast(a as none)from t",
		},
		{
			stmt:     "SELECT CAST (a AS integer) FROM t",
			deparsed: "select cast(a as integer)from t",
		},
		{
			stmt:     "SELECT c1 = c2 COLLATE rtrim FROM t",
			deparsed: "select c1=c2 collate rtrim from t",
		},
		{
			stmt:     "SELECT c1 + 10 FROM t",
			deparsed: "select c1+10 from t",
		},
		{
			stmt:     "SELECT c1 - 10 FROM t",
			deparsed: "select c1-10 from t",
		},
		{
			stmt:     "SELECT c1 * 10 FROM t",
			deparsed: "select c1*10 from t",
		},
		{
			stmt:     "SELECT c1 / 10 FROM t",
			deparsed: "select c1/10 from t",
		},
		{
			stmt:     "SELECT c1 % 10 FROM t",
			deparsed: "select c1%10 from t",
		},
		{
			stmt:     "SELECT c1 & 10 FROM t",
			deparsed: "select c1&10 from t",
		},
		{
			stmt:     "SELECT c1 | 10 FROM t",
			deparsed: "select c1|10 from t",
		},
		{
			stmt:     "GRANT INSERT, UPDATE, DELETE on t TO 'a', 'b'",
			deparsed: "grant delete,insert,update on t to 'a', 'b'",
		},
		{
			stmt:     "REVOKE INSERT, UPDATE, DELETE ON t FROM 'a', 'b'",
			deparsed: "revoke delete,insert,update on t from 'a', 'b'",
		},
		{
			stmt:     "INSERT INTO t (a, b) VALUES (1, 2), (3, 4);",
			deparsed: "insert into t(a,b)values(1,2),(3,4)",
		},
		{
			stmt:     "INSERT INTO t VALUES (1, 2), (3, 4);",
			deparsed: "insert into t values(1,2),(3,4)",
		},
		{
			stmt:     "INSERT INTO t DEFAULT VALUES;",
			deparsed: "insert into t default values",
		},
		{
			stmt:     "DELETE FROM t;",
			deparsed: "delete from t",
		},
		{
			stmt:     "DELETE FROM t WHERE a = 1;",
			deparsed: "delete from t where a=1",
		},
		{
			stmt:     "update t set a = 1, b = 2;",
			deparsed: "update t set a=1,b=2",
		},
		{
			stmt:     "update t set (a, b) = (1, 2);",
			deparsed: "update t set a=1,b=2",
		},
		{
			stmt:     "update t set a = 1, b = 2 where a = 3;",
			deparsed: "update t set a=1,b=2 where a=3",
		},
		{
			stmt:     "CREATE TABLE t (a INT);",
			deparsed: "create table t(a int)",
		},
		{
			stmt:     "CREATE TABLE t (a INT, b INTEGER, c TEXT, d BLOB);",
			deparsed: "create table t(a int,b integer,c text,d blob)",
		},
		{
			stmt:     "CREATE TABLE t (id INT PRIMARY KEY, a INT);",
			deparsed: "create table t(id int primary key,a int)",
		},
		{
			stmt:     "CREATE TABLE t (id INTEGER PRIMARY KEY, a INT);",
			deparsed: "create table t(id integer primary key autoincrement,a int)",
		},
		{
			stmt:     "CREATE TABLE t (id INTEGER PRIMARY KEY ASC, a INT);",
			deparsed: "create table t(id integer primary key asc autoincrement,a int)",
		},
		{
			stmt:     "CREATE TABLE t (id INTEGER PRIMARY KEY DESC, a INT);",
			deparsed: "create table t(id integer primary key desc,a int)",
		},
		{
			stmt:     "CREATE TABLE t (id INT PRIMARY KEY ASC, a INT);",
			deparsed: "create table t(id int primary key asc,a int)",
		},
		{
			stmt:     "CREATE TABLE t (id INT PRIMARY KEY DESC, a INT);",
			deparsed: "create table t(id int primary key desc,a int)",
		},
		{
			stmt:     "CREATE TABLE t (id INT PRIMARY KEY CONSTRAINT nn NOT NULL, id2 INT NOT NULL);",
			deparsed: "create table t(id int primary key constraint nn not null,id2 int not null)",
		},
		{
			stmt:     "CREATE TABLE t (id INT UNIQUE, id2 INT CONSTRAINT un UNIQUE);",
			deparsed: "create table t(id int unique,id2 int constraint un unique)",
		},
		{
			stmt:     "CREATE TABLE t (a INT CHECK(a > 2), id2 INT CONSTRAINT check_constraint CHECK(a > 2));",
			deparsed: "create table t(a int check(a>2),id2 int constraint check_constraint check(a>2))",
		},
		{
			stmt:     "CREATE TABLE t (a INT CONSTRAINT default_constraint DEFAULT 0, b INT DEFAULT 1, c INT DEFAULT 0x1, d TEXT DEFAULT 'foo', e TEXT DEFAULT ('foo'));", // nolint
			deparsed: "create table t(a int constraint default_constraint default 0,b int default 1,c int default 0x1,d text default 'foo',e text default ('foo'))",       // nolint
		},
	}

	wg := sync.WaitGroup{}
	wg.Add(parallelism)
	for i := 0; i < parallelism; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < numIters; j++ {
				tc := tests[rand.Intn(len(tests))]
				tree, err := Parse(tc.stmt)
				require.NoError(t, err)
				require.Equal(t, tc.deparsed, tree.String())
			}
		}()
	}
	wg.Wait()
}

func TestCreateTableMultiplePrimaryKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		stmt string
	}{
		{
			name: "table constraints",
			stmt: "CREATE TABLE t (a INT, b INT, PRIMARY KEY (a), PRIMARY KEY (b));",
		},
		{
			name: "same column constraints",
			stmt: "CREATE TABLE t (a INT PRIMARY KEY PRIMARY KEY);",
		},
		{
			name: "different columns constraints",
			stmt: "CREATE TABLE t (a INT PRIMARY KEY, b INT PRIMARY KEY);",
		},
		{
			name: "mixed constraints",
			stmt: "CREATE TABLE t (a INT PRIMARY KEY, b INT, PRIMARY KEY (b));",
		},
	}

	for _, tc := range tests {
		func(tc struct {
			name string
			stmt string
		},
		) {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				ast, err := Parse(tc.stmt)
				require.Error(t, err)
				require.Len(t, ast.Errors, 1)

				var e *ErrMultiplePrimaryKey
				require.ErrorAs(t, ast.Errors[0], &e)
				require.ErrorAs(t, err, &e)

				// check the stmt in sqlite to make sure sqlite also throws an error
				db, err := sql.Open("sqlite3", "file::"+uuid.NewString()+":?mode=memory&cache=shared&_foreign_keys=on")
				require.NoError(t, err)

				_, err = db.Exec(tc.stmt)
				require.Error(t, err)
				require.ErrorContains(t, err, "has more than one primary key")
			})
		}(tc)
	}
}

func TestRowIDReferences(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		stmt string
	}{
		{
			name: "create table rowid",
			stmt: "CREATE TABLE t (rowid INT);",
		},
		{
			name: "create table _rowid_",
			stmt: "CREATE TABLE t (_rowid_ INT);",
		},
		{
			name: "create table oid",
			stmt: "CREATE TABLE t (oid INT);",
		},
		{
			name: "insert rowid",
			stmt: "INSERT INTO t (rowid) VALUES (1);",
		},
		{
			name: "insert _rowid_",
			stmt: "INSERT INTO t (_rowid_) VALUES (1);",
		},
		{
			name: "insert oid",
			stmt: "INSERT INTO t (oid) VALUES (1);",
		},
		{
			name: "update rowid",
			stmt: "update t set rowid = 1;",
		},
		{
			name: "update _rowid_",
			stmt: "update t set _rowid_ = 1;",
		},
		{
			name: "update oid",
			stmt: "update t set oid = 1;",
		},
		{
			name: "update paren rowid",
			stmt: "update t set (rowid) = (1);",
		},
		{
			name: "update paren _rowid_",
			stmt: "update t set (_rowid_) = (1);",
		},
		{
			name: "update paren oid",
			stmt: "update t set (oid) = (1);",
		},
	}

	for _, tc := range tests {
		func(tc struct {
			name string
			stmt string
		},
		) {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				ast, err := Parse(tc.stmt)
				require.Error(t, err)
				require.Len(t, ast.Errors, 1)

				var e *ErrRowIDNotAllowed
				require.ErrorAs(t, ast.Errors[0], &e)
				require.ErrorAs(t, err, &e)
				require.ErrorContains(t, err, "rowid is not allowed")
			})
		}(tc)
	}
}

func TestInsertWithSelect(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name        string
		stmt        string
		deparsed    string
		expectedAST *AST
		expectedErr interface{}
	}

	tests := []testCase{
		{
			name:     "insert with select without order by",
			stmt:     "INSERT INTO t_1_1 SELECT * FROM t_1_2",
			deparsed: "insert into t_1_1 select * from t_1_2 order by rowid asc",
			expectedAST: &AST{
				Statements: []Statement{
					&Insert{
						Table:   &Table{Name: "t_1_1", IsTarget: true},
						Columns: ColumnList{},
						Rows:    []Exprs{},
						Select: &Select{
							SelectColumnList: SelectColumnList{
								&StarSelectColumn{},
							},
							From: &AliasedTableExpr{
								Expr: &Table{Name: "t_1_2", IsTarget: true},
							},
							OrderBy: OrderBy{
								&OrderingTerm{Expr: &Column{Name: "rowid"}, Direction: AscStr, Nulls: NullsNil},
							},
						},
					},
				},
			},
		},
		{
			name:     "insert with select with order by",
			stmt:     "INSERT INTO t_1_1 SELECT * FROM t_1_2 order by c desc",
			deparsed: "insert into t_1_1 select * from t_1_2 order by c desc,rowid asc",
			expectedAST: &AST{
				Statements: []Statement{
					&Insert{
						Table:   &Table{Name: "t_1_1", IsTarget: true},
						Columns: ColumnList{},
						Rows:    []Exprs{},
						Select: &Select{
							SelectColumnList: SelectColumnList{
								&StarSelectColumn{},
							},
							From: &AliasedTableExpr{
								Expr: &Table{Name: "t_1_2", IsTarget: true},
							},
							OrderBy: OrderBy{
								&OrderingTerm{Expr: &Column{Name: "c"}, Direction: DescStr, Nulls: NullsNil},
								&OrderingTerm{Expr: &Column{Name: "rowid"}, Direction: AscStr, Nulls: NullsNil},
							},
						},
					},
				},
			},
		},
		{
			name:     "insert with select upsert do nothing",
			stmt:     "INSERT INTO t_1_1 SELECT * FROM t_1_2 order by c desc ON CONFLICT DO NOTHING;",
			deparsed: "insert into t_1_1 select * from t_1_2 order by c desc,rowid asc on conflict do nothing",
			expectedAST: &AST{
				Statements: []Statement{
					&Insert{
						Table:   &Table{Name: "t_1_1", IsTarget: true},
						Columns: ColumnList{},
						Rows:    []Exprs{},
						Select: &Select{
							SelectColumnList: SelectColumnList{
								&StarSelectColumn{},
							},
							From: &AliasedTableExpr{
								Expr: &Table{Name: "t_1_2", IsTarget: true},
							},
							OrderBy: OrderBy{
								&OrderingTerm{Expr: &Column{Name: "c"}, Direction: DescStr, Nulls: NullsNil},
								&OrderingTerm{Expr: &Column{Name: "rowid"}, Direction: AscStr, Nulls: NullsNil},
							},
						},
						Upsert: Upsert{
							&OnConflictClause{},
						},
					},
				},
			},
		},
		{
			name: "insert with select group by",
			stmt: `INSERT INTO voting_power (address, ft)
			SELECT owner, SUM(COALESCE(end_time, BLOCK_NUM()) - start_time)
			FROM pilot_sessions GROUP BY owner`,
			deparsed: "insert into voting_power(address,ft)select owner,sum(coalesce(end_time,block_num())-start_time)from pilot_sessions group by owner order by rowid asc", // nolint
			expectedAST: &AST{
				Statements: []Statement{
					&Insert{
						Table: &Table{Name: "voting_power", IsTarget: true},
						Columns: ColumnList{
							&Column{Name: "address"},
							&Column{Name: "ft"},
						},
						Rows: []Exprs{},
						Select: &Select{
							SelectColumnList: SelectColumnList{
								&AliasedSelectColumn{Expr: &Column{Name: "owner"}},
								&AliasedSelectColumn{
									Expr: &FuncExpr{
										Name: "sum",
										Args: Exprs{
											&BinaryExpr{
												Left: &FuncExpr{
													Name: "coalesce",
													Args: Exprs{
														&Column{Name: "end_time"},
														&CustomFuncExpr{
															Name: "block_num",
															Args: Exprs{},
														},
													},
												},
												Operator: MinusStr,
												Right:    &Column{Name: "start_time"},
											},
										},
									},
								},
							},
							From: &AliasedTableExpr{
								Expr: &Table{Name: "pilot_sessions", IsTarget: true},
							},
							GroupBy: GroupBy{
								&Column{Name: "owner"},
							},
							OrderBy: OrderBy{
								&OrderingTerm{Expr: &Column{Name: "rowid"}, Direction: AscStr, Nulls: NullsNil},
							},
						},
					},
				},
			},
		},
		{
			name:     "insert with select having",
			stmt:     "INSERT INTO t_1_1 SELECT a FROM t_1_2 having a > 0",
			deparsed: "insert into t_1_1 select a from t_1_2 having a>0 order by rowid asc",
			expectedAST: &AST{
				Statements: []Statement{
					&Insert{
						Columns: ColumnList{},
						Table:   &Table{Name: "t_1_1", IsTarget: true},
						Rows:    []Exprs{},
						Select: &Select{
							SelectColumnList: SelectColumnList{
								&AliasedSelectColumn{
									Expr: &Column{Name: "a"},
								},
							},
							From: &AliasedTableExpr{
								Expr: &Table{Name: "t_1_2", IsTarget: true},
							},
							Having: &Where{
								Type: HavingStr,
								Expr: &CmpExpr{
									Operator: GreaterThanStr,
									Left:     &Column{Name: "a"},
									Right:    &Value{Type: IntValue, Value: []byte("0")},
								},
							},
							OrderBy: OrderBy{
								&OrderingTerm{Expr: &Column{Name: "rowid"}, Direction: AscStr, Nulls: NullsNil},
							},
						},
					},
				},
			},
		},
		{
			name: "insert with compound select",
			stmt: "INSERT INTO t_1_1 SELECT * FROM t_1_2 UNION SELECT * FROM t_1_3",
			expectedErr: func() **ErrCompoudSelectNotAllowed {
				err := &ErrCompoudSelectNotAllowed{}
				return &err
			}(),
		},
		{
			name: "insert with select with join",
			stmt: "INSERT INTO t_1_1 SELECT * FROM t_1_2, t_1_3",
			expectedErr: func() **ErrContainsJoinTableExpr {
				err := &ErrContainsJoinTableExpr{}
				return &err
			}(),
		},
		{
			name: "insert with select with subselect",
			stmt: "INSERT INTO t_1_1 SELECT * FROM (select * from t_1_2)",
			expectedErr: func() **ErrStatementContainsSubquery {
				err := &ErrStatementContainsSubquery{StatementKind: "insert+select"}
				return &err
			}(),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(tc testCase) func(t *testing.T) {
			return func(t *testing.T) {
				t.Parallel()
				ast, err := Parse(tc.stmt)
				if tc.expectedErr == nil {
					require.NoError(t, err)
					require.Len(t, ast.Errors, 0)
					require.Equal(t, tc.expectedAST, ast)
					require.Equal(t, tc.deparsed, ast.String())
				} else {
					require.ErrorAs(t, ast.Errors[0], tc.expectedErr)
				}
			}
		}(tc))
	}
}

type readResolver struct {
	m map[int]int64
}

func (r *readResolver) GetBlockNumber(chainID int64) (int64, bool) {
	v, ok := r.m[int(chainID)]
	return v, ok
}

type writeResolver struct{}

func (r *writeResolver) GetBlockNumber() int64 {
	return 100
}

func (r *writeResolver) GetTxnHash() string {
	return "0xabc"
}

func TestCustomFunctionResolveReadQuery(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name     string
		query    string
		mustFail bool
		expQuery string
	}

	resolver := &readResolver{
		map[int]int64{1337: 100, 5: 200, 1: 300},
	}

	tests := []testCase{
		{
			name:     "select with block_num(*)",
			query:    "select block_num(1337), block_num(5) from foo_1337_1 where a = block_num(1)",
			expQuery: "select 100,200 from foo_1337_1 where a=300",
		},
		{
			name:     "select with block_num(*) capital letters",
			query:    "select BlOcK_NuM(1337), block_num(5) from foo_1337_1 where a = BLOCK_NUM(1)",
			expQuery: "select 100,200 from foo_1337_1 where a=300",
		},
		{
			name:     "select with block_num() with string argument",
			query:    "select block_num('1337') from foo_1337_1",
			mustFail: true,
		},
		{
			name:     "select with block_num(*) for chainID that doesn't exist",
			query:    "select block_num(1337) from foo_1337_1 where a = block_num(10)",
			mustFail: true,
		},
		{
			name:     "select with txn_hash()",
			query:    "select txn_hash() from foo_1337_1",
			mustFail: true,
		},
		{
			name:     "select with empty block_num()",
			query:    "select block_num() from foo_1337_1",
			mustFail: true,
		},
	}

	for _, it := range tests {
		t.Run(it.name, func(tc testCase) func(t *testing.T) {
			return func(t *testing.T) {
				t.Parallel()

				ast, err := Parse(tc.query)
				require.NoError(t, err)

				for _, stmt := range ast.Statements {
					resolved, err := stmt.(ReadStatement).Resolve(resolver)
					if tc.mustFail {
						require.Error(t, err)
						return
					}
					require.Equal(t, tc.expQuery, resolved)
				}
			}
		}(it))
	}

	t.Run("nil resolver", func(t *testing.T) {
		t.Parallel()

		ast, err := Parse("SELECT block_num(1337) FROM t")
		require.NoError(t, err)

		_, err = ast.Statements[0].(ReadStatement).Resolve(nil)
		require.Error(t, err)
		require.ErrorContains(t, err, "read resolver is needed")
	})

	t.Run("parser level error star", func(t *testing.T) {
		t.Parallel()

		_, err := Parse("SELECT block_num(*) FROM t")
		require.ErrorContains(t, err, "custom function cannot be used with *")
	})

	t.Run("parser level error distinct", func(t *testing.T) {
		t.Parallel()

		_, err := Parse("SELECT block_num(DISTINCT a) FROM t")
		require.ErrorContains(t, err, "custom function cannot have DISTINCT")
	})

	t.Run("parser level error filter", func(t *testing.T) {
		t.Parallel()

		_, err := Parse("SELECT block_num(a) FILTER(WHERE a > 2) FROM t")
		require.ErrorContains(t, err, "custom function cannot have FILTER")
	})
}

func TestCustomFunctionResolveWriteQuery(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name       string
		query      string
		mustFail   bool
		expQueries []string
	}

	tests := []testCase{
		{
			name:       "insert with custom functions",
			query:      "insert into foo_1337_1 values (txn_hash(), block_num())",
			expQueries: []string{"insert into foo_1337_1 values('0xabc',100)"},
		},
		{
			name:       "update with custom functions",
			query:      "update foo_1337_1 SET a=txn_hash(), b=block_num() where c in (block_num(), block_num()+1)",
			expQueries: []string{"update foo_1337_1 set a='0xabc',b=100 where c in(100,100+1)"},
		},
		{
			name:       "delete with custom functions",
			query:      "delete from foo_1337_1 where a=block_num() and b=txn_hash()",
			expQueries: []string{"delete from foo_1337_1 where a=100 and b='0xabc'"},
		},
		{
			name:  "multiple queries",
			query: "insert into foo_1337_1 values (txn_hash()); delete from foo_1337_1 where a=block_num()",
			expQueries: []string{
				"insert into foo_1337_1 values('0xabc')",
				"delete from foo_1337_1 where a=100",
			},
		},
		{
			name:     "block_num() with integer argument",
			query:    "delete from foo_1337_1 where a=block_num(1337)",
			mustFail: true,
		},
		{
			name:     "block_num() with string argument",
			query:    "delete from foo_1337_1 where a=block_num('foo')",
			mustFail: true,
		},
		{
			name:     "txn_hash() with an integer argument",
			query:    "insert into foo_1337_1 values (txn_hash(1))",
			mustFail: true,
		},
		{
			name:     "txn_hash() with a string argument",
			query:    "insert into foo_1337_1 values (txn_hash('foo'))",
			mustFail: true,
		},
	}

	for _, it := range tests {
		t.Run(it.name, func(tc testCase) func(t *testing.T) {
			return func(t *testing.T) {
				t.Parallel()

				ast, err := Parse(tc.query)
				require.NoError(t, err)

				for i, stmt := range ast.Statements {
					resolved, err := stmt.(WriteStatement).Resolve(&writeResolver{})
					if tc.mustFail {
						require.Error(t, err)
						return
					}
					require.Equal(t, tc.expQueries[i], resolved)
				}
			}
		}(it))
	}

	t.Run("nil resolver", func(t *testing.T) {
		t.Parallel()

		ast, err := Parse("insert into foo_1337_1 values (txn_hash())")
		require.NoError(t, err)

		_, err = ast.Statements[0].(WriteStatement).Resolve(nil)
		require.Error(t, err)
		require.ErrorContains(t, err, "write resolver is needed")
	})
}

func TestAlterTable(t *testing.T) {
	type testCase struct {
		name        string
		stmt        string
		deparsed    string
		expectedAST *AST
	}

	tests := []testCase{
		{
			name:     "rename",
			stmt:     "ALTER TABLE t RENAME COLUMN a TO b",
			deparsed: "alter table t rename a to b",
			expectedAST: &AST{
				Statements: []Statement{
					&AlterTable{
						Table: &Table{Name: Identifier("t"), IsTarget: true},
						AlterTableClause: &AlterTableRename{
							OldColumn: &Column{Name: Identifier("a")},
							NewColumn: &Column{Name: Identifier("b")},
						},
					},
				},
			},
		},
		{
			name:     "rename without column keyword",
			stmt:     "ALTER TABLE t RENAME a TO b",
			deparsed: "alter table t rename a to b",
			expectedAST: &AST{
				Statements: []Statement{
					&AlterTable{
						Table: &Table{Name: Identifier("t"), IsTarget: true},
						AlterTableClause: &AlterTableRename{
							OldColumn: &Column{Name: Identifier("a")},
							NewColumn: &Column{Name: Identifier("b")},
						},
					},
				},
			},
		},
		{
			name:     "add",
			stmt:     "ALTER TABLE t ADD COLUMN b int",
			deparsed: "alter table t add b int",
			expectedAST: &AST{
				Statements: []Statement{
					&AlterTable{
						Table: &Table{Name: Identifier("t"), IsTarget: true},
						AlterTableClause: &AlterTableAdd{
							ColumnDef: &ColumnDef{
								Column:      &Column{Name: Identifier("b")},
								Type:        TypeIntStr,
								Constraints: []ColumnConstraint{},
							},
						},
					},
				},
			},
		},
		{
			name:     "add without keyword column",
			stmt:     "ALTER TABLE t ADD b int",
			deparsed: "alter table t add b int",
			expectedAST: &AST{
				Statements: []Statement{
					&AlterTable{
						Table: &Table{Name: Identifier("t"), IsTarget: true},
						AlterTableClause: &AlterTableAdd{
							ColumnDef: &ColumnDef{
								Column:      &Column{Name: Identifier("b")},
								Type:        TypeIntStr,
								Constraints: []ColumnConstraint{},
							},
						},
					},
				},
			},
		},
		{
			name:     "drop",
			stmt:     "ALTER TABLE t DROP COLUMN a",
			deparsed: "alter table t drop a",
			expectedAST: &AST{
				Statements: []Statement{
					&AlterTable{
						Table: &Table{Name: Identifier("t"), IsTarget: true},
						AlterTableClause: &AlterTableDrop{
							Column: &Column{Name: Identifier("a")},
						},
					},
				},
			},
		},
		{
			name:     "drop without keyword column",
			stmt:     "ALTER TABLE t DROP a",
			deparsed: "alter table t drop a",
			expectedAST: &AST{
				Statements: []Statement{
					&AlterTable{
						Table: &Table{Name: Identifier("t"), IsTarget: true},
						AlterTableClause: &AlterTableDrop{
							Column: &Column{Name: Identifier("a")},
						},
					},
				},
			},
		},
	}

	for _, it := range tests {
		t.Run(it.name, func(tc testCase) func(t *testing.T) {
			return func(t *testing.T) {
				t.Parallel()

				ast, err := Parse(tc.stmt)
				require.NoError(t, err)
				require.Equal(t, tc.expectedAST, ast)
				require.Equal(t, tc.deparsed, ast.String())

				// test all ALTER TABLE statements against SQLite3
				db, err := sql.Open("sqlite3", "file::"+uuid.NewString()+":?mode=memory&cache=shared&_foreign_keys=on")
				require.NoError(t, err)

				// create dumm table
				_, err = db.Exec(`CREATE TABLE t (a int, foo int);`)
				require.NoError(t, err)

				_, err = db.Exec(tc.stmt)
				require.NoError(t, err)
				require.NoError(t, db.Close())
			}
		}(it))
	}

	t.Run("primary key constraint check", func(t *testing.T) {
		t.Parallel()

		var expErr *ErrAlterTablePrimaryKeyNotAllowed
		_, err := Parse("alter table t ADD COLUMN a int PRIMARY KEY")
		require.Error(t, err)
		require.ErrorAs(t, err, &expErr)
	})

	t.Run("unique constraint check", func(t *testing.T) {
		t.Parallel()

		var expErr *ErrAlterTableUniqueNotAllowed
		_, err := Parse("alter table t ADD COLUMN a int UNIQUE")
		require.Error(t, err)
		require.ErrorAs(t, err, &expErr)
	})

	t.Run("not null constraint check", func(t *testing.T) {
		t.Parallel()

		var expErr *ErrNotNullConstraintDefaultNotNull
		_, err := Parse("alter table t ADD COLUMN a INT NOT NULL DEFAULT null")
		require.Error(t, err)
		require.ErrorAs(t, err, &expErr)
	})
}

// This is not really a test. It just helps identify which SQLite keywords are reserved and which are not.
func TestReservedKeywords(t *testing.T) {
	// https://www.sqlite.org/lang_keywords.html
	allSQLiteKeywords := []string{"ABORT", "ACTION", "ADD", "AFTER", "ALL", "ALTER", "ALWAYS", "ANALYZE", "AND", "AS", "ASC", "ATTACH", "AUTOINCREMENT", "BEFORE", "BEGIN", "BETWEEN", "BY", "CASCADE", "CASE", "CAST", "CHECK", "COLLATE", "COLUMN", "COMMIT", "CONFLICT", "CONSTRAINT", "CREATE", "CROSS", "CURRENT", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "DATABASE", "DEFAULT", "DEFERRABLE", "DEFERRED", "DELETE", "DESC", "DETACH", "DISTINCT", "DO", "DROP", "EACH", "ELSE", "END", "ESCAPE", "EXCEPT", "EXCLUDE", "EXCLUSIVE", "EXISTS", "EXPLAIN", "FAIL", "FILTER", "FIRST", "FOLLOWING", "FOR", "FOREIGN", "FROM", "FULL", "GENERATED", "GLOB", "GROUP", "GROUPS", "HAVING", "IF", "IGNORE", "IMMEDIATE", "IN", "INDEX", "INDEXED", "INITIALLY", "INNER", "INSERT", "INSTEAD", "INTERSECT", "INTO", "IS", "ISNULL", "JOIN", "KEY", "LAST", "LEFT", "LIKE", "LIMIT", "MATCH", "MATERIALIZED", "NATURAL", "NO", "NOT", "NOTHING", "NOTNULL", "NULL", "NULLS", "OF", "OFFSET", "ON", "OR", "ORDER", "OTHERS", "OUTER", "OVER", "PARTITION", "PLAN", "PRAGMA", "PRECEDING", "PRIMARY", "QUERY", "RAISE", "RANGE", "RECURSIVE", "REFERENCES", "REGEXP", "REINDEX", "RELEASE", "RENAME", "REPLACE", "RESTRICT", "RETURNING", "RIGHT", "ROLLBACK", "ROW", "ROWS", "SAVEPOINT", "SELECT", "SET", "TABLE", "TEMP", "TEMPORARY", "THEN", "TIES", "TO", "TRANSACTION", "TRIGGER", "UNBOUNDED", "UNION", "UNIQUE", "UPDATE", "USING", "VACUUM", "VALUES", "VIEW", "VIRTUAL", "WHEN", "WHERE", "WINDOW", "WITH", "WITHOUT"} // nolint

	for _, keyword := range allSQLiteKeywords {
		// open a different db for each keyword
		db, err := sql.Open("sqlite3", "file::"+uuid.NewString()+":?mode=memory&cache=shared&_foreign_keys=on")
		require.NoError(t, err)

		// create table with keyword as identifier
		_, err = db.Exec(fmt.Sprintf("CREATE TABLE t (%s TEXT)", keyword))
		if err == nil {
			fmt.Printf("%s: unreserved\n", keyword)
		} else {
			fmt.Printf("%s: reserved\n", keyword)
		}

		require.NoError(t, db.Close())
	}
}
