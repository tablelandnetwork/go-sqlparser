package sqlparser

import (
	"fmt"
	"strings"

	"github.com/davecgh/go-spew/spew"
)

// Node represents a node in the AST.
type Node interface {
	ToString() string
}

// AST represents the root Node of the AST.
type AST struct {
	Root Statement
}

func (ast *AST) ToString() string {
	if ast.Root == nil {
		return ""
	}
	return ast.Root.ToString()
}

func (ast *AST) PrettyPrint() {
	spew.Config.DisablePointerAddresses = true
	spew.Dump("%#v", ast)
}

// Statement represents a SQL statement.
type Statement interface {
	iStatement()
	Node
}

func (*Select) iStatement() {}

// SelectStatement any SELECT statement.
type SelectStatement interface {
	iSelectStatement()
	iStatement()
	// AddOrder(*Order)
	// SetLimit(*Limit)
	Node
}

func (*Select) iSelectStatement() {}

// Select represents a SELECT statement.
type Select struct {
	Distinct         string
	SelectColumnList SelectColumnList
	From             TableExprList
	Where            *Where
	GroupBy          GroupBy
	Having           *Where
	Limit            *Limit
	OrderBy          OrderBy
}

// ToString returns the string representation of the node.
func (s *Select) ToString() string {
	return fmt.Sprintf(
		"select %s%s from %s%s%s%s%s%s",
		s.Distinct,
		s.SelectColumnList.ToString(),
		s.From.ToString(),
		s.Where.ToString(),
		s.GroupBy.ToString(),
		s.Having.ToString(),
		s.OrderBy.ToString(),
		s.Limit.ToString(),
	)
}

// Distinct/All
const (
	DistinctStr = "distinct "
	AllStr      = "all "
)

// SelectColumnList represents a list of columns of a SELECT.
type SelectColumnList []SelectColumn

// ToString returns the string representation of the node.
func (cols SelectColumnList) ToString() string {
	var colsStr []string
	for _, rc := range cols {
		colsStr = append(colsStr, rc.ToString())
	}

	return strings.Join(colsStr, ", ")
}

// SelectColumn represents a SELECT column.
type SelectColumn interface {
	iSelectColumn()
	Node
}

func (*StarSelectColumn) iSelectColumn()    {}
func (*AliasedSelectColumn) iSelectColumn() {}

// StarSelectColumn defines a '*' or 'table.*' column.
type StarSelectColumn struct {
	TableRef *Table
}

// ToString returns the string representation of the node.
func (c *StarSelectColumn) ToString() string {
	if c.TableRef != nil {
		return fmt.Sprintf("%s.*", c.TableRef.ToString())
	}
	return "*"
}

// AliasedSelectColumn defines an aliased SELECT column.
type AliasedSelectColumn struct {
	Expr Expr
	As   *Column
}

// ToString returns the string representation of the node.
func (c *AliasedSelectColumn) ToString() string {
	if c.As != nil {
		return fmt.Sprintf("%s as %s", c.Expr.ToString(), c.As.ToString())
	}

	return c.Expr.ToString()
}

// TableExpr represents an expression referenced by FROM.
type TableExpr interface {
	iTableExpr()
	Node
}

func (*AliasedTableExpr) iTableExpr() {}
func (*ParenTableExpr) iTableExpr()   {}
func (*JoinTableExpr) iTableExpr()    {}

// TableExprList represents a list of table expressions.
type TableExprList []TableExpr

// ToString returns the string representation of the node.
func (node TableExprList) ToString() string {
	if len(node) == 0 {
		return ""
	}
	var strs []string
	for _, e := range node {
		strs = append(strs, e.ToString())
	}

	return strings.Join(strs, ", ")
}

// AliasedTableExpr represents a table expression
// coupled with an optional alias.
// If As is empty, no alias was used.
type AliasedTableExpr struct {
	Expr SimpleTableExpr
	As   *Table
}

// ToString returns the string representation of the node.
func (node *AliasedTableExpr) ToString() string {
	if node.As == nil {
		return node.Expr.ToString()
	}

	return fmt.Sprintf("%s as %s", node.Expr.ToString(), node.As.ToString())
}

// SimpleTableExpr represents a direct table reference or a subquery.
type SimpleTableExpr interface {
	iSimpleTableExpr()
	Node
}

func (*Table) iSimpleTableExpr()    {}
func (*Subquery) iSimpleTableExpr() {}

// Subquery represents a subquery.
type Subquery struct {
	Select SelectStatement
}

// ToString returns the string representation of the node.
func (node *Subquery) ToString() string {
	return fmt.Sprintf("(%s)", node.Select.ToString())
}

// ParenTableExpr represents a parenthesized list of TableExpr.
type ParenTableExpr struct {
	TableExprList TableExprList
}

// ToString returns the string representation of the node.
func (node *ParenTableExpr) ToString() string {
	return fmt.Sprintf("(%v)", node.TableExprList.ToString())
}

// JoinTableExpr represents a TableExpr that's a JOIN operation.
type JoinTableExpr struct {
	LeftExpr     TableExpr
	JoinOperator string
	RightExpr    TableExpr
	On           Expr
	Using        ColumnList
}

// Kinds of JoinOperator.
const (
	JoinStr             = "join"
	CrossJoinStr        = "cross join"
	LeftJoinStr         = "left join"
	RightJoinStr        = "right join"
	NaturalJoinStr      = "natural join"
	NaturalLeftJoinStr  = "natural left join"
	NaturalRightJoinStr = "natural right join"
)

// ToString returns the string representation of the node.
func (node *JoinTableExpr) ToString() string {
	if node.On != nil {
		return fmt.Sprintf("%s %s %s on %s", node.LeftExpr.ToString(), node.JoinOperator, node.RightExpr.ToString(), node.On.ToString())
	}

	if node.Using != nil {
		return fmt.Sprintf("%s %s %s using %s", node.LeftExpr.ToString(), node.JoinOperator, node.RightExpr.ToString(), node.Using.ToString())
	}

	return fmt.Sprintf("%s %s %s", node.LeftExpr.ToString(), node.JoinOperator, node.RightExpr.ToString())
}

// Where represents a WHERE or HAVING clause.
type Where struct {
	Type string
	Expr Expr
}

// Types for Where.
const (
	WhereStr  = "where"
	HavingStr = "having"
)

// NewWhere creates a WHERE or HAVING clause out
// of a Expr. If the expression is nil, it returns nil.
func NewWhere(typ string, expr Expr) *Where {
	if expr == nil {
		return nil
	}
	return &Where{Type: typ, Expr: expr}
}

// ToString returns the string representation of the node.
func (w *Where) ToString() string {
	if w == nil || w.Expr == nil {
		return ""
	}
	return fmt.Sprintf(" %s %s", w.Type, w.Expr.ToString())
}

// GroupBy represents a GROUP BY clause.
type GroupBy Exprs

// ToString returns the string representation of the node.
func (node GroupBy) ToString() string {
	if len(node) == 0 {
		return ""
	}
	var strs []string
	for _, e := range node {
		strs = append(strs, e.ToString())
	}

	return fmt.Sprintf(" group by %s", strings.Join(strs, ", "))
}

// OrderBy represents an ORDER BY clause.
type OrderBy []*OrderingTerm

// ToString returns the string representation of the node.
func (node OrderBy) ToString() string {
	if len(node) == 0 {
		return ""
	}
	var strs []string
	for _, e := range node {
		strs = append(strs, e.ToString())
	}

	return fmt.Sprintf(" order by %s", strings.Join(strs, ", "))
}

// OrderingTerm represents an ordering term expression.
type OrderingTerm struct {
	Expr      Expr
	Direction string
	Nulls     NullsType
}

// Possible directions for OrderingTerm.
const (
	AscStr  = "asc"
	DescStr = "desc"
)

// NullsType represents nulls type.
type NullsType int

// All values of NullsType type.
const (
	NullsNil NullsType = iota
	NullsFirst
	NullsLast
)

// ToString returns the string representation of the node.
func (node *OrderingTerm) ToString() string {
	if node, ok := node.Expr.(*NullValue); ok {
		return node.ToString()
	}

	var nullsStr string
	switch node.Nulls {
	case NullsNil:
		nullsStr = ""
	case NullsFirst:
		nullsStr = " nulls first"
	case NullsLast:
		nullsStr = " nulls last"
	}

	return fmt.Sprintf("%s %s%s", node.Expr.ToString(), node.Direction, nullsStr)
}

// Limit represents the LIMIT clause.
type Limit struct {
	Limit  Expr
	Offset Expr
}

// ToString returns the string representation of the node.
func (node *Limit) ToString() string {
	if node == nil {
		return ""
	}

	if node.Offset == nil {
		return fmt.Sprintf(" limit %s", node.Limit.ToString())
	}

	return fmt.Sprintf(" limit %s offset %s", node.Limit.ToString(), node.Offset.ToString())
}

// Expr represents an expr node in the AST.
type Expr interface {
	iExpr()
	Node
}

func (*NullValue) iExpr()   {}
func (BoolValue) iExpr()    {}
func (*Value) iExpr()       {}
func (*UnaryExpr) iExpr()   {}
func (*BinaryExpr) iExpr()  {}
func (*CmpExpr) iExpr()     {}
func (*AndExpr) iExpr()     {}
func (*OrExpr) iExpr()      {}
func (*NotExpr) iExpr()     {}
func (*IsExpr) iExpr()      {}
func (*IsNullExpr) iExpr()  {}
func (*NotNullExpr) iExpr() {}
func (*CollateExpr) iExpr() {}
func (*ConvertExpr) iExpr() {}
func (*BetweenExpr) iExpr() {}
func (*CaseExpr) iExpr()    {}
func (*Column) iExpr()      {}
func (Exprs) iExpr()        {}

// NullValue represents null values.
type NullValue struct{}

// ToString returns the string representation of the node.
func (v *NullValue) ToString() string {
	return "null"
}

// BoolValue represents booleans.
type BoolValue bool

// ToString returns the string representation of the node.
func (v BoolValue) ToString() string {
	if v {
		return "true"
	}

	return "false"
}

// Value represents a single value.
type Value struct {
	Type  ValueType
	Value []byte
}

// ValueType specifies the type for ValueExpr.
type ValueType int

// All possible value types.
const (
	StrValue = ValueType(iota)
	IntValue
	FloatValue
	HexNumValue
	BlobValue
)

// ToString returns the string representation of the node.
func (v *Value) ToString() string {
	var value string
	switch v.Type {
	case StrValue:
		value = fmt.Sprintf("'%s'", string(v.Value))
	case IntValue, FloatValue, HexNumValue:
		value = string(v.Value)
	case BlobValue:
		value = fmt.Sprintf("X'%s'", v.Value)
	}

	return value
}

// UnaryExpr represents a unary value expression.
type UnaryExpr struct {
	Operator string
	Expr     Expr
}

// Operators for UnaryExpr.
const (
	UPlusStr  = "+"
	UMinusStr = "-"
	TildaStr  = "~"
)

// ToString returns the string representation of the node.
func (e *UnaryExpr) ToString() string {
	if expr, ok := e.Expr.(*UnaryExpr); ok {
		return fmt.Sprintf("%s %s", e.Operator, expr.ToString())
	}
	return fmt.Sprintf("%s%s", e.Operator, e.Expr.ToString())
}

// BinaryExpr represents a binary value expression.
type BinaryExpr struct {
	Operator    string
	Left, Right Expr
}

// Operators for BinaryExpr.
const (
	BitAndStr            = "&"
	BitOrStr             = "|"
	PlusStr              = "+"
	MinusStr             = "-"
	MultStr              = "*"
	DivStr               = "/"
	ModStr               = "%"
	ShiftLeftStr         = "<<"
	ShiftRightStr        = ">>"
	ConcatStr            = "||"
	JSONExtractOp        = "->"
	JSONUnquoteExtractOp = "->>"
)

// ToString returns the string representation of the node.
func (e *BinaryExpr) ToString() string {
	return fmt.Sprintf("%s %s %s", e.Left.ToString(), e.Operator, e.Right.ToString())
}

// CmpExpr represents the comparison of two expressions.
type CmpExpr struct {
	Operator    string
	Left, Right Expr
	Escape      Expr
}

// Operators for CmpExpr.
const (
	EqualStr        = "="
	LessThanStr     = "<"
	GreaterThanStr  = ">"
	LessEqualStr    = "<="
	GreaterEqualStr = ">="
	NotEqualStr     = "!="
	InStr           = "in"
	NotInStr        = "not in"
	LikeStr         = "like"
	NotLikeStr      = "not like"
	RegexpStr       = "regexp"
	NotRegexpStr    = "not regexp"
	MatchStr        = "match"
	NotMatchStr     = "not match"
	GlobStr         = "glob"
	NotGlobStr      = "not glob"
)

// ToString returns the string representation of the node.
func (e *CmpExpr) ToString() string {
	if e.Escape != nil {
		return fmt.Sprintf("%s %s %s escape %s", e.Left.ToString(), e.Operator, e.Right.ToString(), e.Escape.ToString())
	}

	return fmt.Sprintf("%s %s %s", e.Left.ToString(), e.Operator, e.Right.ToString())
}

// AndExpr represents an AND expression.
type AndExpr struct {
	Left, Right Expr
}

// ToString returns the string representation of the node.
func (e *AndExpr) ToString() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%s and %s", e.Left.ToString(), e.Right.ToString())
}

// OrExpr represents an OR expression.
type OrExpr struct {
	Left, Right Expr
}

// ToString returns the string representation of the node.
func (e *OrExpr) ToString() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%s or %s", e.Left.ToString(), e.Right.ToString())
}

// NotExpr represents an NOT expression.
type NotExpr struct {
	Expr Expr
}

// ToString returns the string representation of the node.
func (e *NotExpr) ToString() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("not %s", e.Expr.ToString())
}

// IsExpr represents a IS expression
type IsExpr struct {
	Left, Right Expr
}

// ToString returns the string representation of the node.
func (e *IsExpr) ToString() string {
	return fmt.Sprintf("%s is %s", e.Left.ToString(), e.Right.ToString())
}

// IsNullExpr represents a IS expression
type IsNullExpr struct {
	Expr Expr
}

// ToString returns the string representation of the node.
func (e *IsNullExpr) ToString() string {
	return fmt.Sprintf("%s isnull", e.Expr.ToString())
}

// NotNullExpr represents a IS expression
type NotNullExpr struct {
	Expr Expr
}

// ToString returns the string representation of the node.
func (e *NotNullExpr) ToString() string {
	return fmt.Sprintf("%s notnull", e.Expr.ToString())
}

// CollateExpr the COLLATE operator
type CollateExpr struct {
	Expr          Expr
	CollationName string
}

// ToString returns the string representation of the node.
func (e *CollateExpr) ToString() string {
	return fmt.Sprintf("%s collate %s", e.Expr.ToString(), e.CollationName)
}

// ConvertExpr represents a CAST expression.
type ConvertExpr struct {
	Expr Expr
	Type ConvertType
}

// ConvertType specifies the type for ConvertExpr.
type ConvertType string

const (
	NoneStr    = ConvertType("none")
	RealStr    = ConvertType("real")
	NumericStr = ConvertType("numeric")
	TextStr    = ConvertType("text")
	IntegerStr = ConvertType("integer")
)

// ToString returns the string representation of the node.
func (e *ConvertExpr) ToString() string {
	return fmt.Sprintf("cast (%s as %s)", e.Expr.ToString(), string(e.Type))
}

// BetweenExpr represents a BETWEEN or a NOT BETWEEN expression.
type BetweenExpr struct {
	Operator string
	Left     Expr
	From, To Expr
}

// Operators for BetweenExpr.
const (
	BetweenStr    = "between"
	NotBetweenStr = "not between"
)

// ToString returns the string representation of the node.
func (e *BetweenExpr) ToString() string {
	return fmt.Sprintf("%s %s %s and %s", e.Left.ToString(), e.Operator, e.From.ToString(), e.To.ToString())
}

// When represents a WHEN sub-expression.
type When struct {
	Condition Expr
	Value     Expr
}

// ToString returns the string representation of the node.
func (e *When) ToString() string {
	return fmt.Sprintf("when %v then %v", e.Condition.ToString(), e.Value.ToString())
}

// CaseExpr represents a CASE expression.
type CaseExpr struct {
	Expr  Expr
	Whens []*When
	Else  Expr
}

// ToString returns the string representation of the node.
func (e *CaseExpr) ToString() string {
	var b strings.Builder
	b.WriteString("case ")
	if e.Expr != nil {
		b.WriteString(fmt.Sprintf("%s ", e.Expr.ToString()))
	}

	for _, when := range e.Whens {
		b.WriteString(fmt.Sprintf("%s ", when.ToString()))
	}

	if e.Else != nil {
		b.WriteString(fmt.Sprintf("else %s ", e.Else.ToString()))
	}
	b.WriteString("end")
	return b.String()
}

// Table represents a table.
type Table struct {
	Name string
}

// ToString returns the string representation of the node.
func (c *Table) ToString() string {
	return c.Name
}

// Column represents a column.
type Column struct {
	Name     string
	TableRef *Table
}

// ToString returns the string representation of the node.
func (c *Column) ToString() string {
	if c.TableRef != nil {
		return fmt.Sprintf("%s.%s", c.TableRef.ToString(), c.Name)
	}
	return c.Name
}

// ColumnList is a list of columns.
type ColumnList []*Column

// ToString returns the string representation of the node.
func (c ColumnList) ToString() string {
	var strs []string
	for _, e := range c {
		strs = append(strs, e.ToString())
	}

	return fmt.Sprintf("%s%s%s", "(", strings.Join(strs, ", "), ")")
}

// Exprs represents a list of expressions.
type Exprs []Expr

// ToString returns the string representation of the node.
func (c Exprs) ToString() string {
	var strs []string
	for _, e := range c {
		strs = append(strs, e.ToString())
	}

	return fmt.Sprintf("%s%s%s", "(", strings.Join(strs, ", "), ")")
}
