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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
						},
					},
				},
			},
		},
		{
			name:     "string",
			stmt:     "SELECT 'anything betwen single quotes is a string' FROM t",
			deparsed: "select 'anything betwen single quotes is a string' from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
						},
					},
				},
			},
		},
		{
			name:     "float",
			stmt:     "SELECT 1.2 FROM t",
			deparsed: "select 1.2 from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
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
		},
		{
			name:     "float-starts-zero",
			stmt:     "SELECT 0.2 FROM t",
			deparsed: "select 0.2 from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
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
		},
		{
			name:     "float-starts-dot",
			stmt:     "SELECT .2 FROM t",
			deparsed: "select .2 from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
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
		},
		{
			name:     "float-expoent",
			stmt:     "SELECT 1e2 FROM t",
			deparsed: "select 1e2 from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
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
		},
		{
			name:     "float-expoent-upper",
			stmt:     "SELECT 1E2 FROM t",
			deparsed: "select 1E2 from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
						},
					},
				},
			},
		},
		{
			name:     "minus-float",
			stmt:     "SELECT -2.3 FROM t",
			deparsed: "select -2.3 from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
										Expr:     &Column{Name: "a"}},
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
		},
		{
			name:     "comparison-equals",
			stmt:     "SELECT a = 2 FROM t",
			deparsed: "select a = 2 from t",
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
						},
					},
				},
			},
		},
		{
			name:     "comparison-not-equals",
			stmt:     "SELECT a != 2 FROM t",
			deparsed: "select a != 2 from t",
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
						},
					},
				},
			},
		},
		{
			name:     "comparison-not-equals-<>",
			stmt:     "SELECT a <> 2 FROM t",
			deparsed: "select a != 2 from t",
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
						},
					},
				},
			},
		},
		{
			name:     "comparison-greater",
			stmt:     "SELECT a > 2 FROM t",
			deparsed: "select a > 2 from t",
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
						},
					},
				},
			},
		},
		{
			name:     "comparison-less",
			stmt:     "SELECT a < 2 FROM t",
			deparsed: "select a < 2 from t",
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
						},
					},
				},
			},
		},
		{
			name:     "comparison-greater-equal",
			stmt:     "SELECT a >= 2 FROM t",
			deparsed: "select a >= 2 from t",
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
						},
					},
				},
			},
		},
		{
			name:     "comparison-less-equal",
			stmt:     "SELECT a <= 2 FROM t",
			deparsed: "select a <= 2 from t",
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
						},
					},
				},
			},
		},
		{
			name:     "cast-to-text",
			stmt:     "SELECT CAST (1 AS TEXT) FROM t",
			deparsed: "select cast (1 as text) from t",
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
						},
					},
				},
			},
		},
		{
			name:     "cast-to-real",
			stmt:     "SELECT CAST (a AS REAL) FROM t",
			deparsed: "select cast (a as real) from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &ConvertExpr{
									Expr: &Column{
										Name: "a",
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
		},
		{
			name:     "cast-to-none",
			stmt:     "SELECT CAST (a AS none) FROM t",
			deparsed: "select cast (a as none) from t",
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
						},
					},
				},
			},
		},
		{
			name:     "cast-to-numeric",
			stmt:     "SELECT CAST (a AS numeric) FROM t",
			deparsed: "select cast (a as numeric) from t",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: []SelectColumn{
							&AliasedSelectColumn{
								Expr: &ConvertExpr{
									Expr: &Column{
										Name: "a",
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
		},
		{
			name:     "cast-to-integer",
			stmt:     "SELECT CAST (a AS integer) FROM t",
			deparsed: "select cast (a as integer) from t",
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
						},
					},
				},
			},
		},
		{
			name:     "collate",
			stmt:     "SELECT c1 = c2 COLLATE rtrim FROM t",
			deparsed: "select c1 = c2 collate rtrim from t",
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
						},
					},
				},
			},
		},
		{
			name:     "plus",
			stmt:     "SELECT c1 + 10 FROM t",
			deparsed: "select c1 + 10 from t",
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
						},
					},
				},
			},
		},
		{
			name:     "minus",
			stmt:     "SELECT c1 - 10 FROM t",
			deparsed: "select c1 - 10 from t",
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
						},
					},
				},
			},
		},
		{
			name:     "multiplication",
			stmt:     "SELECT c1 * 10 FROM t",
			deparsed: "select c1 * 10 from t",
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
						},
					},
				},
			},
		},
		{
			name:     "division",
			stmt:     "SELECT c1 / 10 FROM t",
			deparsed: "select c1 / 10 from t",
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
						},
					},
				},
			},
		},
		{
			name:     "mod",
			stmt:     "SELECT c1 % 10 FROM t",
			deparsed: "select c1 % 10 from t",
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
						},
					},
				},
			},
		},
		{
			name:     "bitand",
			stmt:     "SELECT c1 & 10 FROM t",
			deparsed: "select c1 & 10 from t",
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
						},
					},
				},
			},
		},
		{
			name:     "bitor",
			stmt:     "SELECT c1 | 10 FROM t",
			deparsed: "select c1 | 10 from t",
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
						},
					},
				},
			},
		},
		{
			name:     "leftshift",
			stmt:     "SELECT c1 << 10 FROM t",
			deparsed: "select c1 << 10 from t",
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
						},
					},
				},
			},
		},
		{
			name:     "rightshift",
			stmt:     "SELECT c1 >> 10 FROM t",
			deparsed: "select c1 >> 10 from t",
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
						},
					},
				},
			},
		},
		{
			name:     "concat",
			stmt:     "SELECT c1 || c2 FROM t",
			deparsed: "select c1 || c2 from t",
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
						},
					},
				},
			},
		},
		{
			name:     "json-extract",
			stmt:     "SELECT c1 -> c2 FROM t",
			deparsed: "select c1 -> c2 from t",
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
						},
					},
				},
			},
		},
		{
			name:     "json-unquote-extract",
			stmt:     "SELECT c1 ->> c2 FROM t",
			deparsed: "select c1 ->> c2 from t",
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
						},
					},
				},
			},
		},
		{
			name:     "parens-expr",
			stmt:     "SELECT a and (a and a and (a or a)) FROM t",
			deparsed: "select a and (a and a and (a or a)) from t",
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
						},
					},
				},
			},
		},
		{
			name:     "simple-select",
			stmt:     "SELECT * FROM t WHERE c1 > c2",
			deparsed: "select * from t where c1 > c2",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&StarSelectColumn{},
						},
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
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
		},
		{
			name:     "multiple-columns",
			stmt:     "SELECT a, t.b, c1 as column, c2 as column2, * FROM t WHERE 1",
			deparsed: "select a, t.b, c1 as column, c2 as column2, * from t where 1",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&AliasedSelectColumn{Expr: &Column{Name: "a"}},
							&AliasedSelectColumn{Expr: &Column{Name: "b", TableRef: &Table{Name: "t"}}},
							&AliasedSelectColumn{Expr: &Column{Name: "c1"}, As: "column"},
							&AliasedSelectColumn{Expr: &Column{Name: "c2"}, As: "column2"},
							&StarSelectColumn{},
						},
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
			name:     "groupby",
			stmt:     "SELECT a, b FROM t GROUP BY a, b",
			deparsed: "select a, b from t group by a, b",
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
			deparsed: "select a, b from t group by a, b having a = 1",
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
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
		},
		{
			name:     "orderby",
			stmt:     "SELECT a, b FROM t ORDER BY a",
			deparsed: "select a, b from t order by a asc",
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
			deparsed: "select a, b from t order by a asc",
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
			deparsed: "select a, b from t order by a desc",
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
			deparsed: "select a, b from t order by a desc, b asc",
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
			deparsed: "select a, b, c from t order by a desc, b asc nulls first, c asc nulls last",
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
								As:   "t",
							},
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
								As:   "t",
							},
						},
					},
				},
			},
		},
		{
			name:     "select-multiple-tables",
			stmt:     "SELECT t.*, t2.c1 as column1 FROM t, t2",
			deparsed: "select t.*, t2.c1 as column1 from t, t2",
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
							&AliasedTableExpr{
								Expr: &Table{Name: "t2"},
							},
						},
					},
				},
			},
		},
		{
			name:     "select-from-subquery",
			stmt:     "SELECT * FROM (SELECT * FROM t)",
			deparsed: "select * from (select * from t)",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
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
												Expr: &Table{Name: "t"},
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
			name:     "select-from-subquery-aliased",
			stmt:     "SELECT * FROM (SELECT * FROM t) as subquery",
			deparsed: "select * from (select * from t) as subquery",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
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
												Expr: &Table{Name: "t"},
											},
										},
									},
								},
								As: "subquery",
							},
						},
					},
				},
			},
		},
		{
			name:     "select-from-subquery-aliased-alt",
			stmt:     "SELECT * FROM (SELECT * FROM t) subquery",
			deparsed: "select * from (select * from t) as subquery",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
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
												Expr: &Table{Name: "t"},
											},
										},
									},
								},
								As: "subquery",
							},
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
						From: TableExprList{
							&JoinTableExpr{
								LeftExpr: &JoinTableExpr{
									LeftExpr: &JoinTableExpr{
										LeftExpr:     &AliasedTableExpr{Expr: &Table{Name: "t"}},
										JoinOperator: JoinStr,
										RightExpr:    &AliasedTableExpr{Expr: &Table{Name: "t2"}},
									},
									JoinOperator: JoinStr,
									RightExpr:    &AliasedTableExpr{Expr: &Table{Name: "t3"}},
								},
								JoinOperator: JoinStr,
								RightExpr:    &AliasedTableExpr{Expr: &Table{Name: "t4"}},
							},
						},
					},
				},
			},
		},
		{
			name:     "join-on",
			stmt:     "SELECT * FROM t JOIN t2 ON t.a = t2.a JOIN t3 ON t2.c1 = t3.c1",
			deparsed: "select * from t join t2 on t.a = t2.a join t3 on t2.c1 = t3.c1",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&StarSelectColumn{},
						},
						From: TableExprList{
							&JoinTableExpr{
								LeftExpr: &JoinTableExpr{
									LeftExpr:     &AliasedTableExpr{Expr: &Table{Name: "t"}},
									JoinOperator: JoinStr,
									RightExpr:    &AliasedTableExpr{Expr: &Table{Name: "t2"}},
									On: &CmpExpr{
										Operator: EqualStr,
										Left:     &Column{Name: "a", TableRef: &Table{Name: "t"}},
										Right:    &Column{Name: "a", TableRef: &Table{Name: "t2"}},
									},
								},
								JoinOperator: JoinStr,
								RightExpr:    &AliasedTableExpr{Expr: &Table{Name: "t3"}},
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
		},
		{
			name:     "join-using",
			stmt:     "SELECT * FROM t JOIN t2 USING(c1, c2)",
			deparsed: "select * from t join t2 using (c1, c2)",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&StarSelectColumn{},
						},
						From: TableExprList{
							&JoinTableExpr{
								LeftExpr:     &AliasedTableExpr{Expr: &Table{Name: "t"}},
								JoinOperator: JoinStr,
								RightExpr:    &AliasedTableExpr{Expr: &Table{Name: "t2"}},
								Using: ColumnList{
									&Column{Name: "c1"},
									&Column{Name: "c2"},
								},
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
						From: TableExprList{
							&AliasedTableExpr{Expr: &Table{Name: "t"}},
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
		},
		{
			name:     "exists",
			stmt:     "SELECT * FROM t WHERE EXISTS (SELECT 1 FROM t2)",
			deparsed: "select * from t where exists (select 1 from t2)",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&StarSelectColumn{},
						},
						From: TableExprList{
							&AliasedTableExpr{Expr: &Table{Name: "t"}},
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
		},
		{
			name:     "not-exists",
			stmt:     "SELECT * FROM t WHERE NOT EXISTS (SELECT 1 FROM t2)",
			deparsed: "select * from t where not exists (select 1 from t2)",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&StarSelectColumn{},
						},
						From: TableExprList{
							&AliasedTableExpr{Expr: &Table{Name: "t"}},
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
		},
		{
			name:     "in empty",
			stmt:     "SELECT a FROM t WHERE a IN ()",
			deparsed: "select a from t where a in ()",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&AliasedSelectColumn{
								Expr: &Column{Name: "a"},
							},
						},
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
			deparsed: "select a from t where a in (1, 2)",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&AliasedSelectColumn{
								Expr: &Column{Name: "a"},
							},
						},
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
			stmt:     "SELECT a FROM t WHERE a IN (SELECT a FROM t2)",
			deparsed: "select a from t where a in (select a from t2)",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&AliasedSelectColumn{
								Expr: &Column{Name: "a"},
							},
						},
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
										From: TableExprList{
											&AliasedTableExpr{
												Expr: &Table{Name: "t2"},
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
			name:     "not in empty",
			stmt:     "SELECT a FROM t WHERE a NOT IN ()",
			deparsed: "select a from t where a not in ()",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&AliasedSelectColumn{
								Expr: &Column{Name: "a"},
							},
						},
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
			deparsed: "select a from t where a not in (1, 2)",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&AliasedSelectColumn{
								Expr: &Column{Name: "a"},
							},
						},
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
			deparsed: "select a from t where a not in (select a from t2)",
			expectedAST: &AST{
				Statements: []Statement{
					&Select{
						SelectColumnList: SelectColumnList{
							&AliasedSelectColumn{
								Expr: &Column{Name: "a"},
							},
						},
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
										From: TableExprList{
											&AliasedTableExpr{
												Expr: &Table{Name: "t2"},
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
			name:     "function call",
			stmt:     "SELECT count(c1) FROM t",
			deparsed: "select count(c1) from t",
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
						},
					},
				},
			},
		},
		{
			name:     "function call filter",
			stmt:     "SELECT max(a) FILTER(WHERE a > 2) FROM t",
			deparsed: "select max(a) filter(where a > 2) from t",
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
						},
					},
				},
			},
		},
		{
			name:     "function call",
			stmt:     "SELECT count(c1) FROM t",
			deparsed: "select count(c1) from t",
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
						},
					},
				},
			},
		},
		{
			name:     "function call star",
			stmt:     "SELECT count(*) FROM t",
			deparsed: "select count(*) from t",
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
						},
					},
				},
			},
		},
		{
			name:        "function does not exist star",
			stmt:        "SELECT foo(*) FROM t",
			deparsed:    "select foo(*) from t",
			expectedAST: nil,
			expectedErr: &ErrNoSuchFunction{FunctionName: "foo"},
		},
		{
			name:     "function call distinct",
			stmt:     "SELECT count(distinct c1) FROM t",
			deparsed: "select count(distinct c1) from t",
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
						},
					},
				},
			},
		},
		{
			name:        "function does not exist",
			stmt:        "SELECT foo(ID) FILTER(WHERE ID > 2) FROM t",
			deparsed:    "select foo(ID) filter(where ID > 2) from t",
			expectedAST: nil,
			expectedErr: &ErrNoSuchFunction{FunctionName: "foo"},
		},
		{
			name:     "function call like with escape",
			stmt:     "SELECT like(a, b, c) FROM t",
			deparsed: "select like(a, b, c) from t",
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
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
					_, err = db.Exec(`CREATE TABLE t (a int, b int, c int, c1 int, c2 int, thisisacolumn int, this_is_a_column3208ADKJHKDS_ int, _also_column int);
						CREATE TABLE t2 (a int, b int, c int, c1 int, c2 int);
						CREATE TABLE t3 (a int, b int, c int, c1 int, c2 int);
						CREATE TABLE t4 (a int, b int, c int, c1 int, c2 int);
					`)
					require.NoError(t, err)

					_, err = db.Exec(tc.stmt)
					require.NoError(t, err)
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
			return "like(a, b)", Exprs{
				&Column{Name: "a"},
				&Column{Name: "b"},
			}
		case "glob":
			return "glob(a, b)", Exprs{
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
	for allowedFunction := range AllowedFunctions {
		functionCall, args := genFunctionCallAndArgs(allowedFunction)
		tests = append(tests, testCase{
			name:     allowedFunction,
			stmt:     fmt.Sprintf("select %s from t", functionCall),
			deparsed: fmt.Sprintf("select %s from t", functionCall),
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
						From: TableExprList{
							&AliasedTableExpr{
								Expr: &Table{Name: "t"},
							},
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
			deparsed:     "CREATE TABLE t (a INT)",
			expectedHash: "0605f6c6705c7c1257edb2d61d94a03ad15f1d253a5a75525c6da8cda34a99ee",
			expectedAST: &AST{
				Statements: []Statement{
					&CreateTable{
						Table:       &Table{Name: "t"},
						Constraints: []TableConstraint{},
						Columns: []*ColumnDef{
							{Name: &Column{Name: "a"}, Type: TypeIntStr, Constraints: []ColumnConstraint{}},
						},
					},
				},
			},
		},
		{
			name:         "create table types",
			stmt:         "CREATE TABLE t (a INT, b INTEGER, c REAL, d TEXT, e BLOB, f ANY);",
			deparsed:     "CREATE TABLE t (a INT, b INTEGER, c REAL, d TEXT, e BLOB, f ANY)",
			expectedHash: "0670bf7a857084333a128354b6f6c6cc1772c9c22bbfeba256c77012fa50fdba",
			expectedAST: &AST{
				Statements: []Statement{
					&CreateTable{
						Table:       &Table{Name: "t"},
						Constraints: []TableConstraint{},
						Columns: []*ColumnDef{
							{Name: &Column{Name: "a"}, Type: TypeIntStr, Constraints: []ColumnConstraint{}},
							{Name: &Column{Name: "b"}, Type: TypeIntegerStr, Constraints: []ColumnConstraint{}},
							{Name: &Column{Name: "c"}, Type: TypeRealStr, Constraints: []ColumnConstraint{}},
							{Name: &Column{Name: "d"}, Type: TypeTextStr, Constraints: []ColumnConstraint{}},
							{Name: &Column{Name: "e"}, Type: TypeBlobStr, Constraints: []ColumnConstraint{}},
							{Name: &Column{Name: "f"}, Type: TypeAnyStr, Constraints: []ColumnConstraint{}},
						},
					},
				},
			},
		},
		{
			name:         "create table primary key",
			stmt:         "CREATE TABLE t (id INT PRIMARY KEY, a INT);",
			deparsed:     "CREATE TABLE t (id INT PRIMARY KEY, a INT)",
			expectedHash: "a360ddb202c0871558c0a3140a67e8a7a7a76e794a297214f7443e8739546408",
			expectedAST: &AST{
				Statements: []Statement{
					&CreateTable{
						Table:       &Table{Name: "t"},
						Constraints: []TableConstraint{},
						Columns: []*ColumnDef{
							{
								Name: &Column{Name: "id"},
								Type: TypeIntStr,
								Constraints: []ColumnConstraint{
									&ColumnConstraintPrimaryKey{Order: ColumnConstraintPrimaryKeyOrderEmpty},
								},
							},
							{Name: &Column{Name: "a"}, Type: TypeIntStr, Constraints: []ColumnConstraint{}},
						},
					},
				},
			},
		},
		{
			name:         "create table primary key asc",
			stmt:         "CREATE TABLE t (id INT PRIMARY KEY ASC, a INT);",
			deparsed:     "CREATE TABLE t (id INT PRIMARY KEY ASC, a INT)",
			expectedHash: "a360ddb202c0871558c0a3140a67e8a7a7a76e794a297214f7443e8739546408",
			expectedAST: &AST{
				Statements: []Statement{
					&CreateTable{
						Table:       &Table{Name: "t"},
						Constraints: []TableConstraint{},
						Columns: []*ColumnDef{
							{
								Name: &Column{Name: "id"},
								Type: TypeIntStr,
								Constraints: []ColumnConstraint{
									&ColumnConstraintPrimaryKey{Order: ColumnConstraintPrimaryKeyOrderAsc},
								},
							},
							{Name: &Column{Name: "a"}, Type: TypeIntStr, Constraints: []ColumnConstraint{}},
						},
					},
				},
			},
		},
		{
			name:         "create table primary key desc",
			stmt:         "CREATE TABLE t (id INT PRIMARY KEY DESC, a INT);",
			deparsed:     "CREATE TABLE t (id INT PRIMARY KEY DESC, a INT)",
			expectedHash: "a360ddb202c0871558c0a3140a67e8a7a7a76e794a297214f7443e8739546408",
			expectedAST: &AST{
				Statements: []Statement{
					&CreateTable{
						Table:       &Table{Name: "t"},
						Constraints: []TableConstraint{},
						Columns: []*ColumnDef{
							{
								Name: &Column{Name: "id"},
								Type: TypeIntStr,
								Constraints: []ColumnConstraint{
									&ColumnConstraintPrimaryKey{Order: ColumnConstraintPrimaryKeyOrderDesc},
								},
							},
							{Name: &Column{Name: "a"}, Type: TypeIntStr, Constraints: []ColumnConstraint{}},
						},
					},
				},
			},
		},
		{
			name:         "create table primary key not null",
			stmt:         "CREATE TABLE t (id INT PRIMARY KEY CONSTRAINT nn NOT NULL, id2 INT NOT NULL);",
			deparsed:     "CREATE TABLE t (id INT PRIMARY KEY CONSTRAINT nn NOT NULL, id2 INT NOT NULL)",
			expectedHash: "43a25e6519b90d5c1303898c3d3883360fcd4559fc0cbabd4015e5de9ab4d1cf",
			expectedAST: &AST{
				Statements: []Statement{
					&CreateTable{
						Table:       &Table{Name: "t"},
						Constraints: []TableConstraint{},
						Columns: []*ColumnDef{
							{
								Name: &Column{Name: "id"},
								Type: TypeIntStr,
								Constraints: []ColumnConstraint{
									&ColumnConstraintPrimaryKey{Order: ColumnConstraintPrimaryKeyOrderEmpty},
									&ColumnConstraintNotNull{
										Name: "nn",
									},
								},
							},
							{
								Name: &Column{Name: "id2"},
								Type: TypeIntStr,
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
			deparsed:     "CREATE TABLE t (id INT UNIQUE, id2 INT CONSTRAINT un UNIQUE)",
			expectedHash: "43a25e6519b90d5c1303898c3d3883360fcd4559fc0cbabd4015e5de9ab4d1cf",
			expectedAST: &AST{
				Statements: []Statement{
					&CreateTable{
						Table:       &Table{Name: "t"},
						Constraints: []TableConstraint{},
						Columns: []*ColumnDef{
							{
								Name: &Column{Name: "id"},
								Type: TypeIntStr,
								Constraints: []ColumnConstraint{
									&ColumnConstraintUnique{},
								},
							},
							{
								Name: &Column{Name: "id2"},
								Type: TypeIntStr,
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
			deparsed:     "CREATE TABLE t (a INT CHECK(a > 2), id2 INT CONSTRAINT check_constraint CHECK(a > 2))",
			expectedHash: "0e93c25832cc90984a0157bdb71f7fa33172700a41c55cd9d896ff4c3d07d598",
			expectedAST: &AST{
				Statements: []Statement{
					&CreateTable{
						Table:       &Table{Name: "t"},
						Constraints: []TableConstraint{},
						Columns: []*ColumnDef{
							{
								Name: &Column{Name: "a"},
								Type: TypeIntStr,
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
								Name: &Column{Name: "id2"},
								Type: TypeIntStr,
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
			stmt:         "CREATE TABLE t (a INT CONSTRAINT default_constraint DEFAULT 0, b INT DEFAULT -1.1, c INT DEFAULT 0x1, d TEXT DEFAULT 'foo', e TEXT DEFAULT ('foo'));",
			deparsed:     "CREATE TABLE t (a INT CONSTRAINT default_constraint DEFAULT 0, b INT DEFAULT -1.1, c INT DEFAULT 0x1, d TEXT DEFAULT 'foo', e TEXT DEFAULT ('foo'))",
			expectedHash: "26c558fd4e4dfb28a9bd399d7872bd24576214a0676eb4a7d3f97362734a03d9",
			expectedAST: &AST{
				Statements: []Statement{
					&CreateTable{
						Table:       &Table{Name: "t"},
						Constraints: []TableConstraint{},
						Columns: []*ColumnDef{
							{
								Name: &Column{Name: "a"},
								Type: TypeIntStr,
								Constraints: []ColumnConstraint{
									&ColumnConstraintDefault{
										Name: "default_constraint",
										Expr: &Value{Type: IntValue, Value: []byte("0")},
									},
								},
							},
							{
								Name: &Column{Name: "b"},
								Type: TypeIntStr,
								Constraints: []ColumnConstraint{
									&ColumnConstraintDefault{
										Expr: &Value{Type: FloatValue, Value: []byte("-1.1")},
									},
								},
							},
							{
								Name: &Column{Name: "c"},
								Type: TypeIntStr,
								Constraints: []ColumnConstraint{
									&ColumnConstraintDefault{
										Expr: &Value{Type: HexNumValue, Value: []byte("0x1")},
									},
								},
							},
							{
								Name: &Column{Name: "d"},
								Type: TypeTextStr,
								Constraints: []ColumnConstraint{
									&ColumnConstraintDefault{
										Expr: &Value{Type: StrValue, Value: []byte("foo")},
									},
								},
							},
							{
								Name: &Column{Name: "e"},
								Type: TypeTextStr,
								Constraints: []ColumnConstraint{
									&ColumnConstraintDefault{
										Expr:        &Value{Type: StrValue, Value: []byte("foo")},
										Parenthesis: true,
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
			stmt:         "CREATE TABLE t (a INTEGER CONSTRAINT pk PRIMARY KEY, b INT, c TEXT, d INT CONSTRAINT gen GENERATED ALWAYS AS (a * abs(b)) VIRTUAL, e TEXT GENERATED ALWAYS AS (substr(c, b, b + 1)) STORED, f TEXT AS (substr(c, b, b + 1)));",
			deparsed:     "CREATE TABLE t (a INTEGER CONSTRAINT pk PRIMARY KEY, b INT, c TEXT, d INT CONSTRAINT gen GENERATED ALWAYS AS (a * abs(b)), e TEXT GENERATED ALWAYS AS (substr(c, b, b + 1)) STORED, f TEXT AS (substr(c, b, b + 1)))",
			expectedHash: "09a0bb453d40af2c8cb23235d92658a73b7e4c0f3688bb8e81c32c48c2266be2",
			expectedAST: &AST{
				Statements: []Statement{
					&CreateTable{
						Table:       &Table{Name: "t"},
						Constraints: []TableConstraint{},
						Columns: []*ColumnDef{
							{
								Name: &Column{Name: "a"},
								Type: TypeIntegerStr,
								Constraints: []ColumnConstraint{
									&ColumnConstraintPrimaryKey{
										Name: "pk",
									},
								},
							},
							{
								Name:        &Column{Name: "b"},
								Type:        TypeIntStr,
								Constraints: []ColumnConstraint{},
							},
							{
								Name:        &Column{Name: "c"},
								Type:        TypeTextStr,
								Constraints: []ColumnConstraint{},
							},
							{
								Name: &Column{Name: "d"},
								Type: TypeIntStr,
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
								Name: &Column{Name: "e"},
								Type: TypeTextStr,
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
								Name: &Column{Name: "f"},
								Type: TypeTextStr,
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
			stmt:         "CREATE TABLE t (id INT CONSTRAINT nm NOT NULL, id2 INT, CONSTRAINT pk PRIMARY KEY (id), CONSTRAINT un UNIQUE (id, id2), CONSTRAINT c CHECK(id > 0));",
			deparsed:     "CREATE TABLE t (id INT CONSTRAINT nm NOT NULL, id2 INT, CONSTRAINT pk PRIMARY KEY (id), CONSTRAINT un UNIQUE (id, id2), CONSTRAINT c CHECK(id > 0))",
			expectedHash: "43a25e6519b90d5c1303898c3d3883360fcd4559fc0cbabd4015e5de9ab4d1cf",
			expectedAST: &AST{
				Statements: []Statement{
					&CreateTable{
						Table: &Table{Name: "t"},
						Columns: []*ColumnDef{
							{
								Name: &Column{Name: "id"},
								Type: TypeIntStr,
								Constraints: []ColumnConstraint{
									&ColumnConstraintNotNull{
										Name: "nm",
									},
								},
							},
							{
								Name:        &Column{Name: "id2"},
								Type:        TypeIntStr,
								Constraints: []ColumnConstraint{},
							},
						},
						Constraints: []TableConstraint{
							&TableConstraintPrimaryKey{
								Name: "pk",
								Columns: ColumnList{
									&Column{Name: "id"},
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
			}
		}(tc))
	}
}

func TestCreateTableStrict(t *testing.T) {
	t.Parallel()
	ast, err := Parse("CREATE TABLE t (a INT);")
	require.NoError(t, err)

	ast.Statements[0].(*CreateTable).StrictMode = true

	require.Equal(t, "CREATE TABLE t (a INT) STRICT", ast.String())
}

func TestInsert(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name        string
		stmt        string
		deparsed    string
		expectedAST *AST
	}

	tests := []testCase{
		{
			name:     "insert simple",
			stmt:     "INSERT INTO t (a, b) VALUES (1, 2), (3, 4);",
			deparsed: "insert into t (a, b) values (1, 2), (3, 4)",
			expectedAST: &AST{
				Statements: []Statement{
					&Insert{
						Table: &Table{Name: "t"},
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
			deparsed: "insert into t values (1, 2), (3, 4)",
			expectedAST: &AST{
				Statements: []Statement{
					&Insert{
						Table:   &Table{Name: "t"},
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
						Table:         &Table{Name: "t"},
						Columns:       ColumnList{},
						Rows:          []Exprs{},
						DefaultValues: true,
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
						Table: &Table{Name: "t"},
					},
				},
			},
		},
		{
			name:     "delete with where",
			stmt:     "DELETE FROM t WHERE a = 1;",
			deparsed: "delete from t where a = 1",
			expectedAST: &AST{
				Statements: []Statement{
					&Delete{
						Table: &Table{Name: "t"},
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
			deparsed: "update t set a = 1, b = 2",
			expectedAST: &AST{
				Statements: []Statement{
					&Update{
						Table: &Table{Name: "t"},
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
			deparsed: "update t set a = 1, b = 2",
			expectedAST: &AST{
				Statements: []Statement{
					&Update{
						Table: &Table{Name: "t"},
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
			deparsed: "update t set a = 1, b = 2 where a = 3",
			expectedAST: &AST{
				Statements: []Statement{
					&Update{
						Table: &Table{Name: "t"},
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
			deparsed: "grant delete, insert, update on t to 'a', 'b'",
			expectedAST: &AST{
				Statements: []Statement{
					&Grant{
						Table: &Table{Name: "t"},
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
			deparsed: "revoke delete, insert, update on t from 'a', 'b'",
			expectedAST: &AST{
				Statements: []Statement{
					&Revoke{
						Table: &Table{Name: "t"},
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
			INSERT INTO t (a, b) VALUES (1, 2), (3, 4);
			delete from t;
			update t set a = 1, b = 2;
			GRANT INSERT, UPDATE, DELETE on t TO 'a', 'b'
			REVOKE INSERT, UPDATE, DELETE ON t FROM 'a', 'b'
			`,
			deparsed: "",
			expectedAST: &AST{
				Statements: []Statement{
					&Insert{
						Table: &Table{Name: "t"},
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
						Table: &Table{Name: "t"},
					},
					&Update{
						Table: &Table{Name: "t"},
						Exprs: []*UpdateExpr{
							{Column: &Column{Name: "a"}, Expr: &Value{Type: IntValue, Value: []byte("1")}},
							{Column: &Column{Name: "b"}, Expr: &Value{Type: IntValue, Value: []byte("2")}},
						},
					},
					&Grant{
						Table: &Table{Name: "t"},
						Privileges: Privileges{
							"insert": struct{}{},
							"update": struct{}{},
							"delete": struct{}{},
						},
						Roles: []string{"a", "b"},
					},
					&Revoke{
						Table: &Table{Name: "t"},
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
		require.Equal(t, "update t set a = 1 where b = 2", updateStmt.String())
	}

	{
		ast, err := Parse("update t SET a = 1 WHERE a = 2")
		require.NoError(t, err)

		updateStmt := ast.Statements[0].(*Update)
		updateStmt.AddWhereClause(where)
		require.Equal(t, "update t set a = 1 where a = 2 and b = 2", updateStmt.String())
	}
	{
		ast, err := Parse("delete from t")
		require.NoError(t, err)

		deleteStmt := ast.Statements[0].(*Delete)
		deleteStmt.AddWhereClause(where)

		require.Equal(t, "delete from t where b = 2", deleteStmt.String())
	}

	{
		ast, err := Parse("delete from t WHERE a = 2")
		require.NoError(t, err)

		deleteStmt := ast.Statements[0].(*Delete)
		deleteStmt.AddWhereClause(where)
		require.Equal(t, "delete from t where a = 2 and b = 2", deleteStmt.String())
	}
}

func TestKeywordsNotAllowed(t *testing.T) {
	t.Parallel()

	for keyword := range keywordsNotAllowed {
		ast, err := Parse(fmt.Sprintf("select %s from t", keyword))
		require.NoError(t, err)
		require.Len(t, ast.Errors, 1)

		var e *ErrKeywordIsNotAllowed
		require.ErrorAs(t, ast.Errors[0], &e)
		if errors.As(ast.Errors[0], &e) {
			require.Equal(t, keyword, e.Keyword)
		}
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
		require.NoError(t, err)
		require.Len(t, ast.Errors, 1)

		var e *ErrTextTooLong
		require.ErrorAs(t, ast.Errors[0], &e)
		if errors.As(ast.Errors[0], &e) {
			require.Equal(t, len(text), e.Length)
			require.Equal(t, MaxBlobLength, e.MaxAllowed)
		}
	})

	t.Run("max blob length", func(t *testing.T) {
		t.Parallel()
		blob := ""
		for i := 0; i <= MaxBlobLength; i++ {
			blob = blob + "f"
		}

		ast, err := Parse(fmt.Sprintf("insert into t (a) values (x'%s')", blob))
		require.NoError(t, err)
		require.Len(t, ast.Errors, 1)

		var e *ErrBlobTooBig
		require.ErrorAs(t, ast.Errors[0], &e)
		if errors.As(ast.Errors[0], &e) {
			require.Equal(t, len(blob), e.Length)
			require.Equal(t, MaxBlobLength, e.MaxAllowed)
		}
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
		require.NoError(t, err)
		require.Len(t, ast.Errors, 1)

		var e *ErrTooManyColumns
		require.ErrorAs(t, ast.Errors[0], &e)
		if errors.As(ast.Errors[0], &e) {
			require.Equal(t, len(columnsDef), e.ColumnCount)
			require.Equal(t, MaxAllowedColumns, e.MaxAllowed)
		}
	})
}

func TestDisallowSubqueriesOnStatements(t *testing.T) {
	t.Parallel()
	t.Run("insert", func(t *testing.T) {
		ast, err := Parse("insert into t (a) VALUES ((select 1 FROM t limit 1))")
		require.NoError(t, err)
		require.Len(t, ast.Errors, 1)

		var e *ErrStatementContainsSubquery
		require.ErrorAs(t, ast.Errors[0], &e)
		if errors.As(ast.Errors[0], &e) {
			require.Equal(t, "insert", e.StatementKind)
		}
	})

	t.Run("update update expr", func(t *testing.T) {
		ast, err := Parse("update t set a = (select 1 FROM t limit 1)")
		require.NoError(t, err)
		require.Len(t, ast.Errors, 1)

		var e *ErrStatementContainsSubquery
		require.ErrorAs(t, ast.Errors[0], &e)
		if errors.As(ast.Errors[0], &e) {
			require.Equal(t, "update", e.StatementKind)
		}
	})

	t.Run("update where", func(t *testing.T) {
		ast, err := Parse("update foo set a=1 where a=(select a from bar limit 1) and b=1")
		require.NoError(t, err)
		require.Len(t, ast.Errors, 1)

		var e *ErrStatementContainsSubquery
		require.ErrorAs(t, ast.Errors[0], &e)
		if errors.As(ast.Errors[0], &e) {
			require.Equal(t, "update", e.StatementKind)
		}
	})

	t.Run("delete", func(t *testing.T) {
		ast, err := Parse("delete from t where a or (select 1 FROM t limit 1)")
		require.NoError(t, err)
		require.Len(t, ast.Errors, 1)

		var e *ErrStatementContainsSubquery
		require.ErrorAs(t, ast.Errors[0], &e)
		if errors.As(ast.Errors[0], &e) {
			require.Equal(t, "delete", e.StatementKind)
		}
	})
}

func TestMultipleErrors(t *testing.T) {
	t.Parallel()
	ast, err := Parse("UPDATE t SET a = (select 1 from t2), b = unknown()")
	require.NoError(t, err)
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
			stmt:     "SELECT 'anything betwen single quotes is a string' FROM t",
			deparsed: "select 'anything betwen single quotes is a string' from t",
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
			stmt:     "SELECT 1.2 FROM t",
			deparsed: "select 1.2 from t",
		},
		{
			stmt:     "SELECT 0.2 FROM t",
			deparsed: "select 0.2 from t",
		},
		{
			stmt:     "SELECT .2 FROM t",
			deparsed: "select .2 from t",
		},
		{
			stmt:     "SELECT 1e2 FROM t",
			deparsed: "select 1e2 from t",
		},
		{
			stmt:     "SELECT 1E2 FROM t",
			deparsed: "select 1E2 from t",
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
			stmt:     "SELECT -2.3 FROM t",
			deparsed: "select -2.3 from t",
		},
		{
			stmt:     "SELECT -a FROM t",
			deparsed: "select -a from t",
		},
		{
			stmt:     "SELECT a = 2 FROM t",
			deparsed: "select a = 2 from t",
		},
		{
			stmt:     "SELECT a != 2 FROM t",
			deparsed: "select a != 2 from t",
		},
		{
			stmt:     "SELECT a < 2 FROM t",
			deparsed: "select a < 2 from t",
		},
		{
			stmt:     "SELECT a >= 2 FROM t",
			deparsed: "select a >= 2 from t",
		},
		{
			stmt:     "SELECT a <= 2 FROM t",
			deparsed: "select a <= 2 from t",
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
			deparsed: "select cast (1 as text) from t",
		},
		{
			stmt:     "SELECT CAST (a AS REAL) FROM t",
			deparsed: "select cast (a as real) from t",
		},
		{
			stmt:     "SELECT CAST (a AS none) FROM t",
			deparsed: "select cast (a as none) from t",
		},
		{
			stmt:     "SELECT CAST (a AS numeric) FROM t",
			deparsed: "select cast (a as numeric) from t",
		},
		{
			stmt:     "SELECT CAST (a AS integer) FROM t",
			deparsed: "select cast (a as integer) from t",
		},
		{
			stmt:     "SELECT c1 = c2 COLLATE rtrim FROM t",
			deparsed: "select c1 = c2 collate rtrim from t",
		},
		{
			stmt:     "SELECT c1 + 10 FROM t",
			deparsed: "select c1 + 10 from t",
		},
		{
			stmt:     "SELECT c1 - 10 FROM t",
			deparsed: "select c1 - 10 from t",
		},
		{
			stmt:     "SELECT c1 * 10 FROM t",
			deparsed: "select c1 * 10 from t",
		},
		{
			stmt:     "SELECT c1 / 10 FROM t",
			deparsed: "select c1 / 10 from t",
		},
		{
			stmt:     "SELECT c1 % 10 FROM t",
			deparsed: "select c1 % 10 from t",
		},
		{
			stmt:     "SELECT c1 & 10 FROM t",
			deparsed: "select c1 & 10 from t",
		},
		{
			stmt:     "SELECT c1 | 10 FROM t",
			deparsed: "select c1 | 10 from t",
		},
		{
			stmt:     "GRANT INSERT, UPDATE, DELETE on t TO 'a', 'b'",
			deparsed: "grant delete, insert, update on t to 'a', 'b'",
		},
		{
			stmt:     "REVOKE INSERT, UPDATE, DELETE ON t FROM 'a', 'b'",
			deparsed: "revoke delete, insert, update on t from 'a', 'b'",
		},
		{
			stmt:     "INSERT INTO t (a, b) VALUES (1, 2), (3, 4);",
			deparsed: "insert into t (a, b) values (1, 2), (3, 4)",
		},
		{
			stmt:     "INSERT INTO t VALUES (1, 2), (3, 4);",
			deparsed: "insert into t values (1, 2), (3, 4)",
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
			deparsed: "delete from t where a = 1",
		},
		{
			stmt:     "update t set a = 1, b = 2;",
			deparsed: "update t set a = 1, b = 2",
		},
		{
			stmt:     "update t set (a, b) = (1, 2);",
			deparsed: "update t set a = 1, b = 2",
		},
		{
			stmt:     "update t set a = 1, b = 2 where a = 3;",
			deparsed: "update t set a = 1, b = 2 where a = 3",
		},
		{
			stmt:     "CREATE TABLE t (a INT);",
			deparsed: "CREATE TABLE t (a INT)",
		},
		{
			stmt:     "CREATE TABLE t (a INT, b INTEGER, c REAL, d TEXT, e BLOB, f ANY);",
			deparsed: "CREATE TABLE t (a INT, b INTEGER, c REAL, d TEXT, e BLOB, f ANY)",
		},
		{
			stmt:     "CREATE TABLE t (id INT PRIMARY KEY, a INT);",
			deparsed: "CREATE TABLE t (id INT PRIMARY KEY, a INT)",
		},
		{
			stmt:     "CREATE TABLE t (id INT PRIMARY KEY ASC, a INT);",
			deparsed: "CREATE TABLE t (id INT PRIMARY KEY ASC, a INT)",
		},
		{
			stmt:     "CREATE TABLE t (id INT PRIMARY KEY DESC, a INT);",
			deparsed: "CREATE TABLE t (id INT PRIMARY KEY DESC, a INT)",
		},
		{
			stmt:     "CREATE TABLE t (id INT PRIMARY KEY CONSTRAINT nn NOT NULL, id2 INT NOT NULL);",
			deparsed: "CREATE TABLE t (id INT PRIMARY KEY CONSTRAINT nn NOT NULL, id2 INT NOT NULL)",
		},
		{
			stmt:     "CREATE TABLE t (id INT UNIQUE, id2 INT CONSTRAINT un UNIQUE);",
			deparsed: "CREATE TABLE t (id INT UNIQUE, id2 INT CONSTRAINT un UNIQUE)",
		},
		{
			stmt:     "CREATE TABLE t (a INT CHECK(a > 2), id2 INT CONSTRAINT check_constraint CHECK(a > 2));",
			deparsed: "CREATE TABLE t (a INT CHECK(a > 2), id2 INT CONSTRAINT check_constraint CHECK(a > 2))",
		},
		{
			stmt:     "CREATE TABLE t (a INT CONSTRAINT default_constraint DEFAULT 0, b INT DEFAULT -1.1, c INT DEFAULT 0x1, d TEXT DEFAULT 'foo', e TEXT DEFAULT ('foo'));",
			deparsed: "CREATE TABLE t (a INT CONSTRAINT default_constraint DEFAULT 0, b INT DEFAULT -1.1, c INT DEFAULT 0x1, d TEXT DEFAULT 'foo', e TEXT DEFAULT ('foo'))",
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
