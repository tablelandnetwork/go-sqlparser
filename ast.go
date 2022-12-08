package sqlparser

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"github.com/davecgh/go-spew/spew"
)

// Node represents a node in the AST.
type Node interface {
	String() string
	walkSubtree(visit Visit) error
}

// AST represents the root Node of the AST.
type AST struct {
	Statements []Statement
	Errors     map[int]error
}

func (node *AST) String() string {
	if len(node.Statements) == 0 {
		return ""
	}

	var stmts []string
	for _, stmt := range node.Statements {
		stmts = append(stmts, stmt.String())
	}
	return strings.Join(stmts, "; ")
}

func (node *AST) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	for _, n := range node.Statements {
		if err := Walk(visit, n); err != nil {
			return err
		}
	}
	return nil
}

func (ast *AST) PrettyPrint() {
	spew.Config.DisablePointerAddresses = true
	spew.Config.DisableMethods = true
	spew.Dump("%#v", ast)
}

// Statement represents a SQL statement.
type Statement interface {
	iStatement()
	Node
}

func (*Select) iStatement()         {}
func (*CompoundSelect) iStatement() {}
func (*CreateTable) iStatement()    {}
func (*Insert) iStatement()         {}
func (*Delete) iStatement()         {}
func (*Update) iStatement()         {}
func (*Grant) iStatement()          {}
func (*Revoke) iStatement()         {}

// ReadStatement is any SELECT statement or UNION statement.
type ReadStatement interface {
	iReadStatement()
	iStatement()
	Node
}

func (*Select) iReadStatement()         {}
func (*CompoundSelect) iReadStatement() {}

// CreateTableStatement is any CREATE TABLE statement.
type CreateTableStatement interface {
	iCreateTableStatement()
	iStatement()
	Node
}

func (*CreateTable) iCreateTableStatement() {}

// WriteStatement is any INSERT, UPDATE or DELETE statement.
type WriteStatement interface {
	iWriteStatement()
	iStatement()
	GetTable() *Table
	Node
}

func (*Insert) iWriteStatement() {}
func (*Update) iWriteStatement() {}
func (*Delete) iWriteStatement() {}

// GrantOrRevokeStatement is any GRANT/REVOKE statement.
type GrantOrRevokeStatement interface {
	iGrantOrRevokeStatement()
	iStatement()
	GetRoles() []string
	GetPrivileges() Privileges
	GetTable() *Table
	Node
}

func (*Grant) iGrantOrRevokeStatement()  {}
func (*Revoke) iGrantOrRevokeStatement() {}

// Select represents a SELECT statement.
type Select struct {
	Distinct         string
	SelectColumnList SelectColumnList
	From             TableExpr
	Where            *Where
	GroupBy          GroupBy
	Having           *Where
	Limit            *Limit
	OrderBy          OrderBy
}

// String returns the string representation of the node.
func (node *Select) String() string {
	return fmt.Sprintf(
		"select %s%s from %s%s%s%s%s%s",
		node.Distinct,
		node.SelectColumnList.String(),
		node.From.String(),
		node.Where.String(),
		node.GroupBy.String(),
		node.Having.String(),
		node.OrderBy.String(),
		node.Limit.String(),
	)
}

func (node *Select) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}

	return Walk(
		visit,
		node.SelectColumnList,
		node.From,
		node.Where,
		node.GroupBy,
		node.Having,
		node.Limit,
		node.OrderBy,
	)
}

// Compound Select operation types
const (
	CompoundUnionStr     = "union"
	CompoundUnionAllStr  = "union all"
	CompoundIntersectStr = "intersect"
	CompoundExceptStr    = "except"
)

// CompoundSelect represents a compound operation of selects.
type CompoundSelect struct {
	Left    *Select
	Type    string
	Right   *Select
	Limit   *Limit
	OrderBy OrderBy
}

func (node *CompoundSelect) String() string {
	return fmt.Sprintf("%s %s %s%s%s", node.Left, node.Type, node.Right, node.Limit, node.OrderBy)
}

func (node *CompoundSelect) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(visit, node.Left, node.Right, node.Limit, node.OrderBy)
}

// Distinct/All
const (
	DistinctStr = "distinct "
	AllStr      = "all "
)

// SelectColumnList represents a list of columns of a SELECT.
type SelectColumnList []SelectColumn

// String returns the string representation of the node.
func (node SelectColumnList) String() string {
	var colsStr []string
	for _, col := range node {
		colsStr = append(colsStr, col.String())
	}

	return strings.Join(colsStr, ", ")
}

func (node SelectColumnList) walkSubtree(visit Visit) error {
	for _, n := range node {
		if err := Walk(visit, n); err != nil {
			return err
		}
	}

	return nil
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

// String returns the string representation of the node.
func (node *StarSelectColumn) String() string {
	if node.TableRef != nil {
		return fmt.Sprintf("%s.*", node.TableRef.String())
	}
	return "*"
}

func (node *StarSelectColumn) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(visit, node.TableRef)
}

// AliasedSelectColumn defines an aliased SELECT column.
type AliasedSelectColumn struct {
	Expr Expr
	As   Identifier
}

// String returns the string representation of the node.
func (node *AliasedSelectColumn) String() string {
	if !node.As.IsEmpty() {
		return fmt.Sprintf("%s as %s", node.Expr.String(), node.As.String())
	}

	return node.Expr.String()
}

func (node *AliasedSelectColumn) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(visit, node.Expr, node.As)
}

// TableExpr represents an expression referenced by FROM.
type TableExpr interface {
	iTableExpr()
	Node
}

func (*AliasedTableExpr) iTableExpr() {}
func (*ParenTableExpr) iTableExpr()   {}
func (*JoinTableExpr) iTableExpr()    {}

// AliasedTableExpr represents a table expression
// coupled with an optional alias.
// If As is empty, no alias was used.
type AliasedTableExpr struct {
	Expr SimpleTableExpr
	As   Identifier
}

// String returns the string representation of the node.
func (node *AliasedTableExpr) String() string {
	if node.As.IsEmpty() {
		return node.Expr.String()
	}

	return fmt.Sprintf("%s as %s", node.Expr.String(), node.As.String())
}

func (node *AliasedTableExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}

	return Walk(visit, node.Expr, node.As)
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
	Select ReadStatement
}

// String returns the string representation of the node.
func (node *Subquery) String() string {
	return fmt.Sprintf("(%s)", node.Select.String())
}

func (node *Subquery) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(visit, node.Select)
}

// ParenTableExpr represents a parenthesized TableExpr.
type ParenTableExpr struct {
	TableExpr TableExpr
}

// String returns the string representation of the node.
func (node *ParenTableExpr) String() string {
	return fmt.Sprintf("(%v)", node.TableExpr.String())
}

func (node *ParenTableExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}

	return Walk(visit, node.TableExpr)
}

// JoinOperator represents a join operator.
type JoinOperator struct {
	Op      string
	Natural bool
	Outer   bool
}

func (node *JoinOperator) String() string {
	var natural string
	if node.Natural {
		natural = "natural "
	}

	if node.Outer {
		node.Op = strings.Replace(node.Op, " ", " outer ", 1)
	}
	return fmt.Sprintf("%s%s", natural, node.Op)
}

func (node *JoinOperator) walkSubtree(visit Visit) error {
	return nil
}

// JoinTableExpr represents a TableExpr that's a JOIN operation.
type JoinTableExpr struct {
	LeftExpr     TableExpr
	JoinOperator *JoinOperator
	RightExpr    TableExpr
	On           Expr
	Using        ColumnList
}

// Kinds of JoinOperator.
const (
	JoinStr = "join"

	LeftJoinStr  = "left join"
	RightJoinStr = "right join"
	FullJoinStr  = "full join"
	InnerJoinStr = "inner join"
)

// String returns the string representation of the node.
func (node *JoinTableExpr) String() string {
	if node.On != nil {
		return fmt.Sprintf("%s %s %s on %s", node.LeftExpr.String(), node.JoinOperator.String(), node.RightExpr.String(), node.On.String())
	}

	if node.Using != nil {
		return fmt.Sprintf("%s %s %s using%s", node.LeftExpr.String(), node.JoinOperator.String(), node.RightExpr.String(), node.Using.String())
	}

	return fmt.Sprintf("%s %s %s", node.LeftExpr.String(), node.JoinOperator.String(), node.RightExpr.String())
}

func (node *JoinTableExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}

	return Walk(visit, node.LeftExpr, node.JoinOperator, node.RightExpr, node.On, node.Using)
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

// String returns the string representation of the node.
func (node *Where) String() string {
	if node == nil || node.Expr == nil {
		return ""
	}
	return fmt.Sprintf(" %s %s", node.Type, node.Expr.String())
}

func (node *Where) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(visit, node.Expr)
}

// GroupBy represents a GROUP BY clause.
type GroupBy Exprs

// String returns the string representation of the node.
func (node GroupBy) String() string {
	if len(node) == 0 {
		return ""
	}
	var strs []string
	for _, e := range node {
		strs = append(strs, e.String())
	}

	return fmt.Sprintf(" group by %s", strings.Join(strs, ", "))
}

func (node GroupBy) walkSubtree(visit Visit) error {
	for _, n := range node {
		if err := Walk(visit, n); err != nil {
			return err
		}
	}

	return nil
}

// OrderBy represents an ORDER BY clause.
type OrderBy []*OrderingTerm

// String returns the string representation of the node.
func (node OrderBy) String() string {
	if len(node) == 0 {
		return ""
	}
	var strs []string
	for _, e := range node {
		strs = append(strs, e.String())
	}

	return fmt.Sprintf(" order by %s", strings.Join(strs, ", "))
}

func (node OrderBy) walkSubtree(visit Visit) error {
	for _, n := range node {
		if err := Walk(visit, n); err != nil {
			return err
		}
	}
	return nil
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

// String returns the string representation of the node.
func (node *OrderingTerm) String() string {
	if node, ok := node.Expr.(*NullValue); ok {
		return node.String()
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

	return fmt.Sprintf("%s %s%s", node.Expr.String(), node.Direction, nullsStr)
}

func (node *OrderingTerm) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(visit, node.Expr)
}

// Limit represents the LIMIT clause.
type Limit struct {
	Limit  Expr
	Offset Expr
}

// String returns the string representation of the node.
func (node *Limit) String() string {
	if node == nil {
		return ""
	}

	if node.Offset == nil {
		return fmt.Sprintf(" limit %s", node.Limit.String())
	}

	return fmt.Sprintf(" limit %s offset %s", node.Limit.String(), node.Offset.String())
}

func (node *Limit) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}

	return Walk(visit, node.Limit, node.Offset)
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
func (*ParenExpr) iExpr()   {}

// NullValue represents null values.
type NullValue struct{}

// String returns the string representation of the node.
func (node *NullValue) String() string {
	return "null"
}

func (node *NullValue) walkSubtree(visit Visit) error {
	return nil
}

// BoolValue represents booleans.
type BoolValue bool

// String returns the string representation of the node.
func (node BoolValue) String() string {
	if node {
		return "true"
	}

	return "false"
}

func (node BoolValue) walkSubtree(visit Visit) error {
	return nil
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

// String returns the string representation of the node.
func (node *Value) String() string {
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

func (node *Value) walkSubtree(visit Visit) error {
	return nil
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

// String returns the string representation of the node.
func (node *UnaryExpr) String() string {
	if expr, ok := node.Expr.(*UnaryExpr); ok {
		return fmt.Sprintf("%s %s", node.Operator, expr.String())
	}
	return fmt.Sprintf("%s%s", node.Operator, node.Expr.String())
}

func (node *UnaryExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(visit, node.Expr)
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

// String returns the string representation of the node.
func (node *BinaryExpr) String() string {
	return fmt.Sprintf("%s %s %s", node.Left.String(), node.Operator, node.Right.String())
}

func (node *BinaryExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(visit, node.Left, node.Right)
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

// String returns the string representation of the node.
func (node *CmpExpr) String() string {
	if node.Escape != nil {
		return fmt.Sprintf("%s %s %s escape %s", node.Left.String(), node.Operator, node.Right.String(), node.Escape.String())
	}

	return fmt.Sprintf("%s %s %s", node.Left.String(), node.Operator, node.Right.String())
}

func (node *CmpExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(visit, node.Left, node.Right, node.Escape)
}

// AndExpr represents an AND expression.
type AndExpr struct {
	Left, Right Expr
}

// String returns the string representation of the node.
func (node *AndExpr) String() string {
	if node == nil {
		return ""
	}
	return fmt.Sprintf("%s and %s", node.Left.String(), node.Right.String())
}

func (node *AndExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}

	return Walk(visit, node.Left, node.Right)
}

// OrExpr represents an OR expression.
type OrExpr struct {
	Left, Right Expr
}

// String returns the string representation of the node.
func (node *OrExpr) String() string {
	if node == nil {
		return ""
	}
	return fmt.Sprintf("%s or %s", node.Left.String(), node.Right.String())
}

func (node *OrExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}

	return Walk(visit, node.Left, node.Right)
}

// NotExpr represents an NOT expression.
type NotExpr struct {
	Expr Expr
}

// String returns the string representation of the node.
func (node *NotExpr) String() string {
	if node == nil {
		return ""
	}
	return fmt.Sprintf("not %s", node.Expr.String())
}

func (node *NotExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}

	return Walk(visit, node.Expr)
}

// IsExpr represents a IS expression
type IsExpr struct {
	Left, Right Expr
}

// String returns the string representation of the node.
func (node *IsExpr) String() string {
	return fmt.Sprintf("%s is %s", node.Left.String(), node.Right.String())
}

func (node *IsExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}

	return Walk(visit, node.Left, node.Right)
}

// IsNullExpr represents a IS expression
type IsNullExpr struct {
	Expr Expr
}

// String returns the string representation of the node.
func (node *IsNullExpr) String() string {
	return fmt.Sprintf("%s isnull", node.Expr.String())
}

func (node *IsNullExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}

	return Walk(visit, node.Expr)
}

// NotNullExpr represents a IS expression
type NotNullExpr struct {
	Expr Expr
}

// String returns the string representation of the node.
func (node *NotNullExpr) String() string {
	return fmt.Sprintf("%s notnull", node.Expr.String())
}

func (node *NotNullExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}

	return Walk(visit, node.Expr)
}

// CollateExpr the COLLATE operator
type CollateExpr struct {
	Expr          Expr
	CollationName Identifier
}

// String returns the string representation of the node.
func (node *CollateExpr) String() string {
	return fmt.Sprintf("%s collate %s", node.Expr.String(), node.CollationName.String())
}

func (node *CollateExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}

	return Walk(visit, node.Expr, node.CollationName)
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
	TextStr    = ConvertType("text")
	IntegerStr = ConvertType("integer")
)

// String returns the string representation of the node.
func (node *ConvertExpr) String() string {
	return fmt.Sprintf("cast (%s as %s)", node.Expr.String(), string(node.Type))
}

func (node *ConvertExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}

	return Walk(visit, node.Expr)
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

// String returns the string representation of the node.
func (node *BetweenExpr) String() string {
	return fmt.Sprintf("%s %s %s and %s", node.Left.String(), node.Operator, node.From.String(), node.To.String())
}

func (node *BetweenExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(visit, node.Left, node.From, node.To)
}

// When represents a WHEN sub-expression.
type When struct {
	Condition Expr
	Value     Expr
}

// String returns the string representation of the node.
func (node *When) String() string {
	return fmt.Sprintf("when %v then %v", node.Condition.String(), node.Value.String())
}

// CaseExpr represents a CASE expression.
type CaseExpr struct {
	Expr  Expr
	Whens []*When
	Else  Expr
}

// String returns the string representation of the node.
func (node *CaseExpr) String() string {
	var b strings.Builder
	b.WriteString("case ")
	if node.Expr != nil {
		b.WriteString(fmt.Sprintf("%s ", node.Expr.String()))
	}

	for _, when := range node.Whens {
		b.WriteString(fmt.Sprintf("%s ", when.String()))
	}

	if node.Else != nil {
		b.WriteString(fmt.Sprintf("else %s ", node.Else.String()))
	}
	b.WriteString("end")
	return b.String()
}

func (node *CaseExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}

	if err := Walk(visit, node.Expr); err != nil {
		return nil
	}

	for _, when := range node.Whens {
		if err := Walk(visit, when.Condition, when.Value); err != nil {
			return err
		}
	}

	return Walk(visit, node.Else)
}

// Table represents a table.
type Table struct {
	Name Identifier

	// IsTarget indicates if the table is a target of a statement or simply a reference.
	IsTarget bool
}

// String returns the string representation of the node.
func (node *Table) String() string {
	return node.Name.String()
}

func (node *Table) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(visit, node.Name)
}

// Column represents a column.
type Column struct {
	Name     Identifier
	TableRef *Table
}

// String returns the string representation of the node.
func (node *Column) String() string {
	if node.TableRef != nil {
		return fmt.Sprintf("%s.%s", node.TableRef.String(), node.Name)
	}
	return node.Name.String()
}

func (node *Column) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}

	return Walk(visit, node.Name, node.TableRef)
}

// ColumnList is a list of columns.
type ColumnList []*Column

// String returns the string representation of the node.
func (node ColumnList) String() string {
	if len(node) == 0 {
		return ""
	}

	var strs []string
	for _, col := range node {
		strs = append(strs, col.String())
	}

	return fmt.Sprintf(" %s%s%s", "(", strings.Join(strs, ", "), ")")
}

func (node ColumnList) walkSubtree(visit Visit) error {
	for _, n := range node {
		if err := Walk(visit, n); err != nil {
			return err
		}
	}

	return nil
}

// IndexedColumn represents a indexed column.
type IndexedColumn struct {
	Column        *Column
	CollationName Identifier
	Order         string
}

// String returns the string representation of the node.
func (node *IndexedColumn) String() string {
	if !node.CollationName.IsEmpty() {
		return fmt.Sprintf("%s COLLATE %s %s", node.Column.String(), node.CollationName, node.Order)
	}

	if node.Order != PrimaryKeyOrderEmpty {
		return fmt.Sprintf("%s %s", node.Column.String(), node.Order)
	}
	return node.Column.String()
}

func (node *IndexedColumn) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}

	return Walk(visit, node.Column, node.CollationName)
}

// IndexedColumnList is a list of indexed columns.
type IndexedColumnList []*IndexedColumn

// String returns the string representation of the node.
func (node IndexedColumnList) String() string {
	if len(node) == 0 {
		return ""
	}

	var strs []string
	for _, col := range node {
		strs = append(strs, col.String())
	}

	return fmt.Sprintf(" %s%s%s", "(", strings.Join(strs, ", "), ")")
}

func (node IndexedColumnList) walkSubtree(visit Visit) error {
	for _, n := range node {
		if err := Walk(visit, n); err != nil {
			return err
		}
	}

	return nil
}

// Exprs represents a list of expressions.
type Exprs []Expr

// String returns the string representation of the node.
func (node Exprs) String() string {
	var strs []string
	for _, expr := range node {
		strs = append(strs, expr.String())
	}

	return fmt.Sprintf("%s%s%s", "(", strings.Join(strs, ", "), ")")
}

func (node Exprs) walkSubtree(visit Visit) error {
	for _, n := range node {
		if err := Walk(visit, n); err != nil {
			return err
		}
	}

	return nil
}

// ExistsExpr represents a EXISTS expression.
type ExistsExpr struct {
	Subquery *Subquery
}

// String returns the string representation of the node.
func (node *ExistsExpr) String() string {
	return fmt.Sprintf("exists %s", node.Subquery.String())
}

func (node *ExistsExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}

	return Walk(visit, node.Subquery)
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
	Name     Identifier
	Distinct bool
	Args     Exprs
	Filter   *Where
}

// String returns the string representation of the node.
func (node *FuncExpr) String() string {
	var distinct string
	if node.Distinct {
		distinct = "distinct "
	}

	var filter string
	if node.Filter != nil {
		filter = fmt.Sprintf(" filter(%s)", node.Filter.String()[1:])
	}

	var argsStr string
	if node.Args != nil {
		argsStr = node.Args.String()
	} else {
		argsStr = "(*)"
	}

	return fmt.Sprintf("%s%s%s", node.Name.String(), argsStr[:1]+distinct+argsStr[1:], filter)
}

func (node *FuncExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}

	return Walk(visit, node.Name, node.Args, node.Filter)
}

// ParenExpr represents a (expr) expression.
type ParenExpr struct {
	Expr Expr
}

// String returns the string representation of the node.
func (node *ParenExpr) String() string {
	return fmt.Sprintf("(%s)", node.Expr.String())
}

func (node *ParenExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(visit, node.Expr)
}

// Identifier represents a Column, Table and Function name identifier.
type Identifier string

// String returns the string representation of the node.
func (node Identifier) String() string {
	return string(node)
}

func (node Identifier) walkSubtree(visit Visit) error {
	return nil
}

// IsEmpty returns if the identifier is empty.
func (node Identifier) IsEmpty() bool {
	return node == ""
}

// CreateTable represents a CREATE TABLE statement.
type CreateTable struct {
	Table       *Table
	ColumnsDef  []*ColumnDef
	Constraints []TableConstraint

	// This is the only TableOption supported in the AST.
	// The grammar cannot parse this option.
	// It is used to toggle the strict mode directiy in the AST.
	StrictMode bool
}

// String returns the string representation of the node.
func (node *CreateTable) String() string {
	columns := []string{}
	for _, column := range node.ColumnsDef {
		columns = append(columns, column.String())
	}
	column := strings.Join(columns, ", ")
	if len(node.Constraints) > 0 {
		constraints := []string{}
		for _, constraint := range node.Constraints {
			constraints = append(constraints, constraint.String())
		}
		column += ", " + strings.Join(constraints, ", ")
	}

	if node.StrictMode {
		return fmt.Sprintf("create table %s (%s) strict", node.Table.String(), column)
	} else {
		return fmt.Sprintf("create table %s (%s)", node.Table.String(), column)
	}
}

func (node *CreateTable) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}

	if err := Walk(visit, node.Table); err != nil {
		return err
	}

	for _, n := range node.ColumnsDef {
		if err := Walk(visit, n); err != nil {
			return err
		}
	}

	for _, n := range node.Constraints {
		if err := Walk(visit, n); err != nil {
			return err
		}
	}

	return nil
}

// StructureHash returns the hash of the structure of the statement.
func (node *CreateTable) StructureHash() string {
	cols := make([]string, len(node.ColumnsDef))
	for i := range node.ColumnsDef {
		cols[i] = fmt.Sprintf("%s:%s", node.ColumnsDef[i].Column.String(), strings.ToUpper(node.ColumnsDef[i].Type))
	}
	stringifiedColDef := strings.Join(cols, ",")
	sh := sha256.New()
	sh.Write([]byte(stringifiedColDef))
	hash := sh.Sum(nil)
	return hex.EncodeToString(hash)
}

// ColumnDef represents the column definition of a CREATE TABLE statement.
type ColumnDef struct {
	Column      *Column
	Type        string
	Constraints []ColumnConstraint
}

// String returns the string representation of the node.
func (node *ColumnDef) String() string {
	constraint := ""
	if len(node.Constraints) > 0 {
		constraints := []string{}
		for _, constraint := range node.Constraints {
			constraints = append(constraints, constraint.String())
		}
		constraint = " " + strings.Join(constraints, " ")
	}
	return fmt.Sprintf("%s %s%s", node.Column, node.Type, constraint)
}

func (node *ColumnDef) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}

	if err := Walk(visit, node.Column); err != nil {
		return err
	}

	for _, n := range node.Constraints {
		if err := Walk(visit, n); err != nil {
			return err
		}
	}

	return nil
}

// HasPrimaryKey checks if column definition has a primary key constraint.
func (node *ColumnDef) HasPrimaryKey() bool {
	for _, constraint := range node.Constraints {
		if _, ok := constraint.(*ColumnConstraintPrimaryKey); ok {
			return true
		}
	}

	return false
}

// Types for ColumnDef type.
const (
	TypeIntStr     = "int"
	TypeIntegerStr = "integer"
	TypeTextStr    = "text"
	TypeBlobStr    = "blob"
)

// ColumnConstraint is used for parsing column constraint info from SQL.
type ColumnConstraint interface {
	iColumnConstraint()
	Node
}

func (*ColumnConstraintPrimaryKey) iColumnConstraint() {}
func (*ColumnConstraintNotNull) iColumnConstraint()    {}
func (*ColumnConstraintUnique) iColumnConstraint()     {}
func (*ColumnConstraintCheck) iColumnConstraint()      {}
func (*ColumnConstraintDefault) iColumnConstraint()    {}
func (*ColumnConstraintGenerated) iColumnConstraint()  {}

// ColumnConstraintPrimaryKey represents a PRIMARY KEY column constraint for CREATE TABLE.
type ColumnConstraintPrimaryKey struct {
	Name          Identifier
	Order         string
	AutoIncrement bool
	// ConflictClause *ConflictClause
}

// String returns the string representation of the node.
func (node *ColumnConstraintPrimaryKey) String() string {
	var constraintName string
	if !node.Name.IsEmpty() {
		constraintName = fmt.Sprintf("constraint %s ", node.Name.String())
	}

	var autoIncrement string
	if node.AutoIncrement {
		autoIncrement = " autoincrement"
	}

	if node.Order == PrimaryKeyOrderEmpty {
		return fmt.Sprintf("%sprimary key%s", constraintName, autoIncrement)
	}
	return fmt.Sprintf("%sprimary key %s%s", constraintName, node.Order, autoIncrement)
}

func (node *ColumnConstraintPrimaryKey) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}

	return Walk(visit, node.Name)
}

const (
	PrimaryKeyOrderEmpty = ""
	PrimaryKeyOrderAsc   = "asc"
	PrimaryKeyOrderDesc  = "desc"
)

// ColumnConstraintNotNull represents a NOT NULL column constraint for CREATE TABLE.
type ColumnConstraintNotNull struct {
	Name Identifier
	// ConflictClause *ConflictClause
}

// String returns the string representation of the node.
func (node *ColumnConstraintNotNull) String() string {
	var constraintName string
	if !node.Name.IsEmpty() {
		constraintName = fmt.Sprintf("constraint %s ", node.Name.String())
	}
	return fmt.Sprintf("%snot null", constraintName)
}

func (node *ColumnConstraintNotNull) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}

	return Walk(visit, node.Name)
}

// ColumnConstraintUnique represents a UNIQUE column constraint for CREATE TABLE.
type ColumnConstraintUnique struct {
	Name Identifier
	// ConflictClause *ConflictClause
}

// String returns the string representation of the node.
func (node *ColumnConstraintUnique) String() string {
	var constraintName string
	if !node.Name.IsEmpty() {
		constraintName = fmt.Sprintf("constraint %s ", node.Name.String())
	}
	return fmt.Sprintf("%sunique", constraintName)
}

func (node *ColumnConstraintUnique) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}

	return Walk(visit, node.Name)
}

// ColumnConstraintCheck represents a CHECK column constraint for CREATE TABLE.
type ColumnConstraintCheck struct {
	Name Identifier
	Expr Expr
}

// String returns the string representation of the node.
func (node *ColumnConstraintCheck) String() string {
	var constraintName string
	if !node.Name.IsEmpty() {
		constraintName = fmt.Sprintf("constraint %s ", node.Name.String())
	}
	return fmt.Sprintf("%scheck(%s)", constraintName, node.Expr.String())
}

func (node *ColumnConstraintCheck) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}

	return Walk(visit, node.Name, node.Expr)
}

// ColumnConstraintDefault represents a DEFAULT column constraint for CREATE TABLE.
type ColumnConstraintDefault struct {
	Name        Identifier
	Expr        Expr
	Parenthesis bool
}

// String returns the string representation of the node.
func (node *ColumnConstraintDefault) String() string {
	var constraintName string
	if !node.Name.IsEmpty() {
		constraintName = fmt.Sprintf("constraint %s ", node.Name.String())
	}
	if node.Parenthesis {
		return fmt.Sprintf("%sdefault (%s)", constraintName, node.Expr.String())
	}
	return fmt.Sprintf("%sdefault %s", constraintName, node.Expr.String())
}

func (node *ColumnConstraintDefault) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}

	return Walk(visit, node.Name, node.Expr)
}

// ColumnConstraintGenerated represents a GENERATED ALWAYS column constraint for CREATE TABLE.
type ColumnConstraintGenerated struct {
	Name Identifier
	Expr Expr

	// the GENERATED ALWAYS keywords are optional in the grammar.
	GeneratedAlways bool

	// this is a flag for VIRTUAL or STORED keywords.
	IsStored bool
}

// String returns the string representation of the node.
func (node *ColumnConstraintGenerated) String() string {
	var constraintName string
	if !node.Name.IsEmpty() {
		constraintName = fmt.Sprintf("constraint %s ", node.Name.String())
	}
	var b strings.Builder
	if node.GeneratedAlways {
		b.WriteString(fmt.Sprintf("%sgenerated always as (%s)", constraintName, node.Expr.String()))
	} else {
		b.WriteString(fmt.Sprintf("%sas (%s)", constraintName, node.Expr.String()))
	}

	if node.IsStored {
		b.WriteString(" stored")
	}

	return b.String()
}

func (node *ColumnConstraintGenerated) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}

	return Walk(visit, node.Name, node.Expr)
}

type TableConstraint interface {
	iTableConstraint()
	Node
}

func (*TableConstraintPrimaryKey) iTableConstraint() {}
func (*TableConstraintUnique) iTableConstraint()     {}
func (*TableConstraintCheck) iTableConstraint()      {}

// TableConstraintPrimaryKey is a PRIMARY KEY constraint for table definition.
type TableConstraintPrimaryKey struct {
	Name    Identifier
	Columns IndexedColumnList
}

// String returns the string representation of the node.
func (node *TableConstraintPrimaryKey) String() string {
	var constraintName string
	if !node.Name.IsEmpty() {
		constraintName = fmt.Sprintf("constraint %s ", node.Name.String())
	}

	return fmt.Sprintf("%sprimary key%s", constraintName, node.Columns.String())
}

func (node *TableConstraintPrimaryKey) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}

	return Walk(visit, node.Name, node.Columns)
}

// TableConstraintUnique is a UNIQUE constraint for table definition.
type TableConstraintUnique struct {
	Name    Identifier
	Columns ColumnList
}

// String returns the string representation of the node.
func (node *TableConstraintUnique) String() string {
	var constraintName string
	if !node.Name.IsEmpty() {
		constraintName = fmt.Sprintf("constraint %s ", node.Name.String())
	}

	return fmt.Sprintf("%sunique%s", constraintName, node.Columns.String())
}

func (node *TableConstraintUnique) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}

	return Walk(visit, node.Name, node.Columns)
}

// TableConstraintCheck is a CHECK constraint for table definition.
type TableConstraintCheck struct {
	Name Identifier
	Expr Expr
}

// String returns the string representation of the node.
func (node *TableConstraintCheck) String() string {
	var constraintName string
	if !node.Name.IsEmpty() {
		constraintName = fmt.Sprintf("constraint %s ", node.Name.String())
	}

	return fmt.Sprintf("%scheck(%s)", constraintName, node.Expr.String())
}

func (node *TableConstraintCheck) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}

	return Walk(visit, node.Name, node.Expr)
}

// Insert represents an INSERT statement.
type Insert struct {
	Table         *Table
	Columns       ColumnList
	Rows          []Exprs
	DefaultValues bool
	Upsert        Upsert
	Select        *Select

	// RETURNING clause is not accepted in the parser.
	ReturningClause Exprs
}

// GetTable returns the table.
func (node *Insert) GetTable() *Table {
	return node.Table
}

// String returns the string representation of the node.
func (node *Insert) String() string {
	var returning string
	if node.ReturningClause != nil {
		returning = fmt.Sprintf(" returning %s", node.ReturningClause.String())
	}

	if node.Select != nil {
		return fmt.Sprintf("insert into %s %s%s%s", node.Table.Name.String(), node.Select.String(), node.Upsert.String(), returning)
	}

	if node.DefaultValues {
		return fmt.Sprintf("insert into %s default values%s", node.Table.Name.String(), returning)
	}

	var rows []string
	for _, row := range node.Rows {
		rows = append(rows, row.String())
	}
	return fmt.Sprintf("insert into %s%s values %s%s%s", node.Table.String(), node.Columns.String(), strings.Join(rows, ", "), node.Upsert.String(), returning)
}

func (node *Insert) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}

	if err := Walk(visit, node.Table, node.Columns); err != nil {
		return err
	}

	for _, n := range node.Rows {
		if err := Walk(visit, n); err != nil {
			return err
		}
	}

	return Walk(visit, node.Upsert)
}

// Upsert represents an upsert clause, which is a list of on conflict clause.
type Upsert []*OnConflictClause

func (node Upsert) String() string {
	if len(node) == 0 {
		return ""
	}

	var clauses []string
	for _, clause := range node {
		clauses = append(clauses, fmt.Sprintf("on conflict%s", clause.String()))
	}

	return fmt.Sprintf(" %s", strings.Join(clauses, " "))
}

func (node Upsert) walkSubtree(visit Visit) error {
	for _, n := range node {
		if err := Walk(visit, n); err != nil {
			return nil
		}
	}

	return nil
}

type OnConflictClause struct {
	Target   *OnConflictTarget
	DoUpdate *OnConflictUpdate
}

func (node *OnConflictClause) String() string {
	var target string
	if node.Target != nil {
		target = fmt.Sprintf("%s%s", node.Target.Columns.String(), node.Target.Where.String())
	}

	if node.DoUpdate == nil {
		return fmt.Sprintf("%s do nothing", target)
	}

	return fmt.Sprintf("%s do update set %s%s", target, node.DoUpdate.Exprs.String(), node.DoUpdate.Where.String())
}

func (node *OnConflictClause) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(visit, node.Target.Columns, node.Target.Where, node.DoUpdate.Exprs, node.DoUpdate.Where)
}

type OnConflictTarget struct {
	Columns ColumnList
	Where   *Where
}

type OnConflictUpdate struct {
	Exprs UpdateExprs
	Where *Where
}

// Delete represents an DELETE statement.
type Delete struct {
	Table *Table
	Where *Where
}

// String returns the string representation of the node.
func (node *Delete) String() string {
	return fmt.Sprintf("delete from %s%s", node.Table.String(), node.Where.String())
}

// GetTable returns the table.
func (node *Delete) GetTable() *Table {
	return node.Table
}

// AddWhereClause add a WHERE clause to DELETE.
func (node *Delete) AddWhereClause(where *Where) {
	if node.Where == nil {
		node.Where = where
		return
	}

	node.Where = &Where{
		Type: WhereStr,
		Expr: &AndExpr{
			Left:  node.Where.Expr,
			Right: where.Expr,
		},
	}
}

func (node *Delete) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(visit, node.Table, node.Where)
}

// Update represents an UPDATE statement.
type Update struct {
	Table *Table
	Exprs UpdateExprs
	Where *Where

	// RETURNING clause is not accepted in the parser.
	ReturningClause Exprs
}

// String returns the string representation of the node.
func (node *Update) String() string {
	var returning string
	if node.ReturningClause != nil {
		returning = fmt.Sprintf(" returning %s", node.ReturningClause.String())
	}

	return fmt.Sprintf("update %s set %s%s%s", node.Table.String(), node.Exprs.String(), node.Where.String(), returning)
}

// GetTable returns the table.
func (node *Update) GetTable() *Table {
	return node.Table
}

func (node *Update) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(visit, node.Table, node.Exprs, node.Where)
}

// AddWhereClause add a WHERE clause to UPDATE.
func (node *Update) AddWhereClause(where *Where) {
	if node.Where == nil {
		node.Where = where
		return
	}

	node.Where = &Where{
		Type: WhereStr,
		Expr: &AndExpr{
			Left:  node.Where.Expr,
			Right: where.Expr,
		},
	}
}

type UpdateExprs []*UpdateExpr

func (node UpdateExprs) String() string {
	var exprs []string
	for _, expr := range node {
		exprs = append(exprs, fmt.Sprintf("%s = %s", expr.Column.String(), expr.Expr.String()))
	}

	return strings.Join(exprs, ", ")
}

func (node UpdateExprs) walkSubtree(visit Visit) error {
	for _, n := range node {
		if err := Walk(visit, n.Column, n.Expr); err != nil {
			return nil
		}
	}
	return nil
}

// UpdateExpr represents an UPDATE SET expression (Column = Expr).
type UpdateExpr struct {
	Column *Column
	Expr   Expr
}

// Grant represents a GRANT statement.
type Grant struct {
	Privileges Privileges
	Table      *Table
	Roles      []string
}

// String returns the string representation of the node.
func (node *Grant) String() string {
	return fmt.Sprintf("grant %s on %s to %s", node.Privileges.String(), node.Table.String(), "'"+strings.Join(node.Roles, "', '")+"'")
}

// GetRoles returns the roles.
func (node *Grant) GetRoles() []string {
	return node.Roles
}

// GetTable returns the table.
func (node *Grant) GetTable() *Table {
	return node.Table
}

// GetPrivileges returns the privileges.
func (node *Grant) GetPrivileges() Privileges {
	return node.Privileges
}

func (node *Grant) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(visit, node.Privileges, node.Table)
}

// Privileges represents the GRANT privilges (INSERT, UPDATE, DELETE).
type Privileges map[string]struct{}

// String returns the string representation of the node.
func (node Privileges) String() string {
	var privileges []string
	for priv := range node {
		privileges = append(privileges, priv)
	}

	// we cannot guarantee map order, so we sort it so the string is deterministic
	sort.Strings(privileges)
	return strings.Join(privileges, ", ")
}

func (node Privileges) Len() int {
	return len(node)
}

func (node Privileges) walkSubtree(visit Visit) error {
	return nil
}

// Revoke represents a REVOKE statement.
type Revoke struct {
	Privileges Privileges
	Table      *Table
	Roles      []string
}

// String returns the string representation of the node.
func (node *Revoke) String() string {
	return fmt.Sprintf("revoke %s on %s from %s", node.Privileges.String(), node.Table.String(), "'"+strings.Join(node.Roles, "', '")+"'")
}

// GetRoles returns the roles.
func (node *Revoke) GetRoles() []string {
	return node.Roles
}

// GetTable returns the table.
func (node *Revoke) GetTable() *Table {
	return node.Table
}

// GetPrivileges returns the privileges.
func (node *Revoke) GetPrivileges() Privileges {
	return node.Privileges
}

func (node *Revoke) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(visit, node.Privileges, node.Table)
}
