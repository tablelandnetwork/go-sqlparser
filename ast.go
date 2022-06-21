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

func (node *AST) ToString() string {
	if node.Root == nil {
		return ""
	}
	return node.Root.ToString()
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
func (node *Select) ToString() string {
	return fmt.Sprintf(
		"select %s%s from %s%s%s%s%s%s",
		node.Distinct,
		node.SelectColumnList.ToString(),
		node.From.ToString(),
		node.Where.ToString(),
		node.GroupBy.ToString(),
		node.Having.ToString(),
		node.OrderBy.ToString(),
		node.Limit.ToString(),
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
func (node SelectColumnList) ToString() string {
	var colsStr []string
	for _, col := range node {
		colsStr = append(colsStr, col.ToString())
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
func (node *StarSelectColumn) ToString() string {
	if node.TableRef != nil {
		return fmt.Sprintf("%s.*", node.TableRef.ToString())
	}
	return "*"
}

// AliasedSelectColumn defines an aliased SELECT column.
type AliasedSelectColumn struct {
	Expr Expr
	As   *Column
}

// ToString returns the string representation of the node.
func (node *AliasedSelectColumn) ToString() string {
	if node.As != nil {
		return fmt.Sprintf("%s as %s", node.Expr.ToString(), node.As.ToString())
	}

	return node.Expr.ToString()
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
func (node *Where) ToString() string {
	if node == nil || node.Expr == nil {
		return ""
	}
	return fmt.Sprintf(" %s %s", node.Type, node.Expr.ToString())
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
func (*Subquery) iExpr()    {}
func (*ExistsExpr) iExpr()  {}
func (*FuncExpr) iExpr()    {}

// NullValue represents null values.
type NullValue struct{}

// ToString returns the string representation of the node.
func (node *NullValue) ToString() string {
	return "null"
}

// BoolValue represents booleans.
type BoolValue bool

// ToString returns the string representation of the node.
func (node BoolValue) ToString() string {
	if node {
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
func (node *Value) ToString() string {
	var value string
	switch node.Type {
	case StrValue:
		value = fmt.Sprintf("'%s'", string(node.Value))
	case IntValue, FloatValue, HexNumValue:
		value = string(node.Value)
	case BlobValue:
		value = fmt.Sprintf("X'%s'", node.Value)
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
func (node *UnaryExpr) ToString() string {
	if expr, ok := node.Expr.(*UnaryExpr); ok {
		return fmt.Sprintf("%s %s", node.Operator, expr.ToString())
	}
	return fmt.Sprintf("%s%s", node.Operator, node.Expr.ToString())
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
func (node *BinaryExpr) ToString() string {
	return fmt.Sprintf("%s %s %s", node.Left.ToString(), node.Operator, node.Right.ToString())
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
func (node *CmpExpr) ToString() string {
	if node.Escape != nil {
		return fmt.Sprintf("%s %s %s escape %s", node.Left.ToString(), node.Operator, node.Right.ToString(), node.Escape.ToString())
	}

	return fmt.Sprintf("%s %s %s", node.Left.ToString(), node.Operator, node.Right.ToString())
}

// AndExpr represents an AND expression.
type AndExpr struct {
	Left, Right Expr
}

// ToString returns the string representation of the node.
func (node *AndExpr) ToString() string {
	if node == nil {
		return ""
	}
	return fmt.Sprintf("%s and %s", node.Left.ToString(), node.Right.ToString())
}

// OrExpr represents an OR expression.
type OrExpr struct {
	Left, Right Expr
}

// ToString returns the string representation of the node.
func (node *OrExpr) ToString() string {
	if node == nil {
		return ""
	}
	return fmt.Sprintf("%s or %s", node.Left.ToString(), node.Right.ToString())
}

// NotExpr represents an NOT expression.
type NotExpr struct {
	Expr Expr
}

// ToString returns the string representation of the node.
func (node *NotExpr) ToString() string {
	if node == nil {
		return ""
	}
	return fmt.Sprintf("not %s", node.Expr.ToString())
}

// IsExpr represents a IS expression
type IsExpr struct {
	Left, Right Expr
}

// ToString returns the string representation of the node.
func (node *IsExpr) ToString() string {
	return fmt.Sprintf("%s is %s", node.Left.ToString(), node.Right.ToString())
}

// IsNullExpr represents a IS expression
type IsNullExpr struct {
	Expr Expr
}

// ToString returns the string representation of the node.
func (node *IsNullExpr) ToString() string {
	return fmt.Sprintf("%s isnull", node.Expr.ToString())
}

// NotNullExpr represents a IS expression
type NotNullExpr struct {
	Expr Expr
}

// ToString returns the string representation of the node.
func (node *NotNullExpr) ToString() string {
	return fmt.Sprintf("%s notnull", node.Expr.ToString())
}

// CollateExpr the COLLATE operator
type CollateExpr struct {
	Expr          Expr
	CollationName string
}

// ToString returns the string representation of the node.
func (node *CollateExpr) ToString() string {
	return fmt.Sprintf("%s collate %s", node.Expr.ToString(), node.CollationName)
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
func (node *ConvertExpr) ToString() string {
	return fmt.Sprintf("cast (%s as %s)", node.Expr.ToString(), string(node.Type))
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
func (node *BetweenExpr) ToString() string {
	return fmt.Sprintf("%s %s %s and %s", node.Left.ToString(), node.Operator, node.From.ToString(), node.To.ToString())
}

// When represents a WHEN sub-expression.
type When struct {
	Condition Expr
	Value     Expr
}

// ToString returns the string representation of the node.
func (node *When) ToString() string {
	return fmt.Sprintf("when %v then %v", node.Condition.ToString(), node.Value.ToString())
}

// CaseExpr represents a CASE expression.
type CaseExpr struct {
	Expr  Expr
	Whens []*When
	Else  Expr
}

// ToString returns the string representation of the node.
func (node *CaseExpr) ToString() string {
	var b strings.Builder
	b.WriteString("case ")
	if node.Expr != nil {
		b.WriteString(fmt.Sprintf("%s ", node.Expr.ToString()))
	}

	for _, when := range node.Whens {
		b.WriteString(fmt.Sprintf("%s ", when.ToString()))
	}

	if node.Else != nil {
		b.WriteString(fmt.Sprintf("else %s ", node.Else.ToString()))
	}
	b.WriteString("end")
	return b.String()
}

// Table represents a table.
type Table struct {
	Name string
}

// ToString returns the string representation of the node.
func (node *Table) ToString() string {
	return node.Name
}

// Column represents a column.
type Column struct {
	Name     string
	TableRef *Table
}

// ToString returns the string representation of the node.
func (node *Column) ToString() string {
	if node.TableRef != nil {
		return fmt.Sprintf("%s.%s", node.TableRef.ToString(), node.Name)
	}
	return node.Name
}

// ColumnList is a list of columns.
type ColumnList []*Column

// ToString returns the string representation of the node.
func (node ColumnList) ToString() string {
	var strs []string
	for _, col := range node {
		strs = append(strs, col.ToString())
	}

	return fmt.Sprintf("%s%s%s", "(", strings.Join(strs, ", "), ")")
}

// Exprs represents a list of expressions.
type Exprs []Expr

// ToString returns the string representation of the node.
func (node Exprs) ToString() string {
	var strs []string
	for _, expr := range node {
		strs = append(strs, expr.ToString())
	}

	return fmt.Sprintf("%s%s%s", "(", strings.Join(strs, ", "), ")")
}

// ExistsExpr represents a EXISTS expression.
type ExistsExpr struct {
	Subquery *Subquery
}

// ToString returns the string representation of the node.
func (node *ExistsExpr) ToString() string {
	return fmt.Sprintf("exists %s", node.Subquery.ToString())
}

// ColTuple represents a list of column values for IN operator.
// It can be ValTuple or Subquery.
type ColTuple interface {
	iColTuple()
	Expr
}

func (Exprs) iColTuple()     {}
func (*Subquery) iColTuple() {}

// FuncExpr represents a function call.
type FuncExpr struct {
	Name     *Column
	Distinct bool
	Args     Exprs
	Filter   *Where
}

// ToString returns the string representation of the node.
func (node *FuncExpr) ToString() string {
	var distinct string
	if node.Distinct {
		distinct = "distinct "
	}

	var filter string
	if node.Filter != nil {
		filter = fmt.Sprintf(" filter(%s)", node.Filter.ToString()[1:])
	}

	var argsStr string
	if node.Args != nil {
		argsStr = node.Args.ToString()
	} else {
		argsStr = "(*)"
	}

	return fmt.Sprintf("%s%s%s", node.Name.ToString(), argsStr[:1]+distinct+argsStr[1:], filter)
}
