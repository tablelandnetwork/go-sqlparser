package sqlparser

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
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
	return strings.Join(stmts, ";")
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

// PrettyPrint prints the AST.
func (node *AST) PrettyPrint() {
	spew.Config.DisablePointerAddresses = true
	spew.Config.DisableMethods = true
	spew.Dump("%#v", node)
}

// Combines an ordered set of two node strings with correct delimiting.
func nodeStringConcat(left string, right string) string {
	// If a node string starts or ends with any of these bytes the string will never
	// need to be space delimited when being concataneted with other node strings
	noDelim := map[byte]struct{}{
		'=': {},
		'<': {},
		'>': {},
		'!': {},
		'(': {},
		')': {},
	}

	left = strings.Trim(left, " ")
	right = strings.Trim(right, " ")

	// If one of the strings is empty we don't want to add an unnecessary space and
	// we can't access index the `-1` below, so we can just return the other string
	if left == "" {
		return right
	}
	if right == "" {
		return left
	}

	if _, ok := noDelim[left[len(left)-1]]; ok {
		return fmt.Sprintf("%s%s", left, right)
	}
	if _, ok := noDelim[right[0]]; ok {
		return fmt.Sprintf("%s%s", left, right)
	}

	return fmt.Sprintf("%s %s", left, right)
}

func nodeStringsConcat(strs ...string) string {
	var subtreeString string
	for _, str := range strs {
		subtreeString = nodeStringConcat(subtreeString, str)
	}
	return subtreeString
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
func (*AlterTable) iStatement()     {}

// ReadStatementResolver resolves Tableland Custom Functions for a read statement.
type ReadStatementResolver interface {
	// GetBlockNumber returns the last known block number for the provided chainID. If the chainID isn't known,
	// it returns (0, false).
	GetBlockNumber(chainID int64) (int64, bool)

	// GetBindValues returns a slice of values to be bound to their respective parameters.
	GetBindValues() []Expr
}

// WriteStatementResolver resolves Tableland Custom Functions for a write statement.
type WriteStatementResolver interface {
	// GetTxnHash returns the transaction hash of the transaction containing the query being processed.
	GetTxnHash() string

	// GetBlockNumber returns the block number of the block containing query being processed.
	GetBlockNumber() int64
}

// ReadStatement is any SELECT statement or UNION statement.
type ReadStatement interface {
	Statement
	iReadStatement()

	// Resolve returns a string representation with custom function nodes resolved to the values
	// passed by resolver.
	Resolve(ReadStatementResolver) (string, error)
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
	Statement
	iWriteStatement()
	GetTable() *Table

	// Resolve returns a string representation with custom function nodes resolved to the values
	// passed by resolver.
	Resolve(WriteStatementResolver) (string, error)
}

func (*Insert) iWriteStatement()     {}
func (*Update) iWriteStatement()     {}
func (*Delete) iWriteStatement()     {}
func (*AlterTable) iWriteStatement() {}

// GrantOrRevokeStatement is any GRANT/REVOKE statement.
type GrantOrRevokeStatement interface {
	Statement
	iGrantOrRevokeStatement()
	GetRoles() []string
	GetPrivileges() Privileges
	GetTable() *Table
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
	return nodeStringsConcat(
		"select",
		node.Distinct,
		node.SelectColumnList.String(),
		"from",
		node.From.String(),
		node.Where.String(),
		node.GroupBy.String(),
		node.Having.String(),
		node.OrderBy.String(),
		node.Limit.String(),
	)
}

// Resolve returns a string representation with custom function nodes resolved to the values
// passed by resolver.
func (node *Select) Resolve(resolver ReadStatementResolver) (string, error) {
	return resolveReadStatementWalk(node, resolver)
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

// Compound Select operation types.
const (
	CompoundUnionStr     = "union"
	CompoundUnionAllStr  = "union all"
	CompoundIntersectStr = "intersect"
	CompoundExceptStr    = "except"
)

// CompoundSelect represents a compound operation of selects.
type CompoundSelect struct {
	Left  *Select
	Type  string
	Right ReadStatement
}

func (node *CompoundSelect) String() string {
	return nodeStringsConcat(
		node.Left.String(),
		node.Type,
		node.Right.String(),
	)
}

// Resolve returns a string representation with custom function nodes resolved to the values
// passed by resolver.
func (node *CompoundSelect) Resolve(resolver ReadStatementResolver) (string, error) {
	return resolveReadStatementWalk(node, resolver)
}

func (node *CompoundSelect) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(visit, node.Left, node.Right)
}

// Distinct/All.
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

	return strings.Join(colsStr, ",")
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
		return nodeStringsConcat(node.Expr.String(), "as", node.As.String())
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

	return nodeStringsConcat(node.Expr.String(), "as", node.As.String())
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
	return nodeStringsConcat("(", node.Select.String(), ")")
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
	return nodeStringsConcat("(", node.TableExpr.String(), ")")
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
	return nodeStringsConcat(natural, node.Op)
}

func (node *JoinOperator) walkSubtree(_ Visit) error {
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
		return nodeStringsConcat(
			node.LeftExpr.String(),
			node.JoinOperator.String(),
			node.RightExpr.String(),
			"on",
			node.On.String(),
		)
	}

	if node.Using != nil {
		return nodeStringsConcat(
			node.LeftExpr.String(),
			node.JoinOperator.String(),
			node.RightExpr.String(),
			"using",
			node.Using.String(),
		)
	}

	return nodeStringsConcat(
		node.LeftExpr.String(),
		node.JoinOperator.String(),
		node.RightExpr.String(),
	)
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

	return nodeStringsConcat("group by", strings.Join(strs, ","))
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

	return nodeStringsConcat("order by", strings.Join(strs, ","))
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
		nullsStr = "nulls first"
	case NullsLast:
		nullsStr = "nulls last"
	}

	return nodeStringsConcat(node.Expr.String(), node.Direction, nullsStr)
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
		return nodeStringsConcat("limit", node.Limit.String())
	}

	return nodeStringsConcat("limit", node.Limit.String(), "offset", node.Offset.String())
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

func (*NullValue) iExpr()      {}
func (BoolValue) iExpr()       {}
func (*Value) iExpr()          {}
func (*UnaryExpr) iExpr()      {}
func (*BinaryExpr) iExpr()     {}
func (*CmpExpr) iExpr()        {}
func (*AndExpr) iExpr()        {}
func (*OrExpr) iExpr()         {}
func (*NotExpr) iExpr()        {}
func (*IsExpr) iExpr()         {}
func (*IsNullExpr) iExpr()     {}
func (*NotNullExpr) iExpr()    {}
func (*CollateExpr) iExpr()    {}
func (*ConvertExpr) iExpr()    {}
func (*BetweenExpr) iExpr()    {}
func (*CaseExpr) iExpr()       {}
func (*Column) iExpr()         {}
func (Exprs) iExpr()           {}
func (*Subquery) iExpr()       {}
func (*ExistsExpr) iExpr()     {}
func (*FuncExpr) iExpr()       {}
func (*CustomFuncExpr) iExpr() {}
func (*ParenExpr) iExpr()      {}

// NullValue represents null values.
type NullValue struct{}

// String returns the string representation of the node.
func (node *NullValue) String() string {
	return "null"
}

func (node *NullValue) walkSubtree(_ Visit) error {
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

func (node BoolValue) walkSubtree(_ Visit) error {
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

func (node *Value) walkSubtree(_ Visit) error {
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
	return fmt.Sprintf("%s%s%s", node.Left.String(), node.Operator, node.Right.String())
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
		return nodeStringsConcat(node.Left.String(), node.Operator, node.Right.String(), "escape", node.Escape.String())
	}

	return nodeStringsConcat(node.Left.String(), node.Operator, node.Right.String())
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
	return nodeStringsConcat(node.Left.String(), "and", node.Right.String())
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
	return nodeStringsConcat(node.Left.String(), "or", node.Right.String())
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
	return nodeStringsConcat("not", node.Expr.String())
}

func (node *NotExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}

	return Walk(visit, node.Expr)
}

// IsExpr represents a IS expression.
type IsExpr struct {
	Left, Right Expr
}

// String returns the string representation of the node.
func (node *IsExpr) String() string {
	return nodeStringsConcat(node.Left.String(), "is", node.Right.String())
}

func (node *IsExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}

	return Walk(visit, node.Left, node.Right)
}

// IsNullExpr represents a IS expression.
type IsNullExpr struct {
	Expr Expr
}

// String returns the string representation of the node.
func (node *IsNullExpr) String() string {
	return nodeStringsConcat(node.Expr.String(), "isnull")
}

func (node *IsNullExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}

	return Walk(visit, node.Expr)
}

// NotNullExpr represents a IS expression.
type NotNullExpr struct {
	Expr Expr
}

// String returns the string representation of the node.
func (node *NotNullExpr) String() string {
	return nodeStringsConcat(node.Expr.String(), "notnull")
}

func (node *NotNullExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}

	return Walk(visit, node.Expr)
}

// CollateExpr the COLLATE operator.
type CollateExpr struct {
	Expr          Expr
	CollationName Identifier
}

// String returns the string representation of the node.
func (node *CollateExpr) String() string {
	return nodeStringsConcat(node.Expr.String(), "collate", node.CollationName.String())
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
	// NoneStr NONE convert type.
	NoneStr = ConvertType("none")

	// TextStr TEXT convert type.
	TextStr = ConvertType("text")

	// IntegerStr INTEGER convert type.
	IntegerStr = ConvertType("integer")
)

// String returns the string representation of the node.
func (node *ConvertExpr) String() string {
	return nodeStringsConcat("cast(", node.Expr.String(), "as", string(node.Type), ")")
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
	return nodeStringsConcat(node.Left.String(), node.Operator, node.From.String(), "and", node.To.String())
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
	return nodeStringsConcat("when", node.Condition.String(), "then", node.Value.String())
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

	return nodeStringsConcat("(", strings.Join(strs, ","), ")")
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

	return nodeStringsConcat("(", strings.Join(strs, ","), ")")
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

	return nodeStringsConcat("(", strings.Join(strs, ","), ")")
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
	return nodeStringsConcat("exists", node.Subquery.String())
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
		filter = nodeStringsConcat("filter(", node.Filter.String()[1:], ")")
	}

	var argsStr string
	if node.Args != nil {
		argsStr = node.Args.String()
	} else {
		argsStr = "(*)"
	}

	return nodeStringsConcat(node.Name.String(), argsStr[:1]+distinct+argsStr[1:], filter)
}

func (node *FuncExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}

	return Walk(visit, node.Name, node.Args, node.Filter)
}

// CustomFuncExpr represents a function call.
type CustomFuncExpr struct {
	Name           Identifier
	Args           Exprs
	ResolvedString string
}

// String returns the string representation of the node.
func (node *CustomFuncExpr) String() string {
	if node.ResolvedString != "" {
		return node.ResolvedString
	}

	var argsStr string
	if node.Args != nil {
		argsStr = node.Args.String()
	} else {
		argsStr = "(*)"
	}

	return nodeStringsConcat(node.Name.String(), argsStr[:1]+argsStr[1:])
}

func (node *CustomFuncExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}

	return Walk(visit, node.Name, node.Args)
}

// ParenExpr represents a (expr) expression.
type ParenExpr struct {
	Expr Expr
}

// String returns the string representation of the node.
func (node *ParenExpr) String() string {
	return nodeStringsConcat("(", node.Expr.String(), ")")
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

func (node Identifier) walkSubtree(_ Visit) error {
	return nil
}

// IsEmpty returns if the identifier is empty.
func (node Identifier) IsEmpty() bool {
	return node == ""
}

// Param represents a question mark (?) parameter.
type Param struct {
	ResolvedString string
}

func (node *Param) iExpr() {}

// String returns the string representation of the node.
func (node *Param) String() string {
	if node.ResolvedString != "" {
		return node.ResolvedString
	}
	return "?"
}

func (node *Param) walkSubtree(_ Visit) error {
	return nil
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
	column := strings.Join(columns, ",")
	if len(node.Constraints) > 0 {
		constraints := []string{}
		for _, constraint := range node.Constraints {
			constraints = append(constraints, constraint.String())
		}
		column += "," + strings.Join(constraints, ",")
	}

	if node.StrictMode {
		return nodeStringsConcat("create table ", node.Table.String(), "(", column, ")strict")
	}

	return nodeStringsConcat("create table ", node.Table.String(), "(", column, ")")
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
	return nodeStringsConcat(node.Column.String(), node.Type, constraint)
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
		constraintName = nodeStringsConcat("constraint", node.Name.String())
	}

	var autoIncrement string
	if node.AutoIncrement {
		autoIncrement = "autoincrement"
	}

	if node.Order == PrimaryKeyOrderEmpty {
		return nodeStringsConcat(constraintName, "primary key", autoIncrement)
	}
	return nodeStringsConcat(constraintName, "primary key", node.Order, autoIncrement)
}

func (node *ColumnConstraintPrimaryKey) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}

	return Walk(visit, node.Name)
}

const (
	// PrimaryKeyOrderEmpty no primary key order.
	PrimaryKeyOrderEmpty = ""

	// PrimaryKeyOrderAsc primary key asc order.
	PrimaryKeyOrderAsc = "asc"

	// PrimaryKeyOrderDesc primary key desc order.
	PrimaryKeyOrderDesc = "desc"
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
		constraintName = nodeStringsConcat("constraint", node.Name.String())
	}
	return nodeStringsConcat(constraintName, "not null")
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
		constraintName = nodeStringsConcat("constraint", node.Name.String())
	}
	return nodeStringsConcat(constraintName, "unique")
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
		constraintName = nodeStringsConcat("constraint", node.Name.String())
	}
	return nodeStringsConcat(constraintName, "check(", node.Expr.String(), ")")
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
		constraintName = nodeStringsConcat("constraint", node.Name.String())
	}
	if node.Parenthesis {
		return nodeStringsConcat(constraintName, "default (", node.Expr.String(), ")")
	}
	return nodeStringsConcat(constraintName, "default", node.Expr.String())
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
		constraintName = nodeStringsConcat("constraint", node.Name.String())
	}
	var b strings.Builder
	if node.GeneratedAlways {
		b.WriteString(nodeStringsConcat(constraintName, "generated always as(", node.Expr.String(), ")"))
	} else {
		b.WriteString(nodeStringsConcat(constraintName, "as(", node.Expr.String(), ")"))
	}

	bStr := b.String()
	if node.IsStored {
		return nodeStringsConcat(bStr, "stored")
	}

	return bStr
}

func (node *ColumnConstraintGenerated) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}

	return Walk(visit, node.Name, node.Expr)
}

// TableConstraint is a contrainst applied to the whole table in a CREATE TABLE statement.
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
		constraintName = nodeStringsConcat("constraint", node.Name.String())
	}

	return nodeStringsConcat(constraintName, "primary key", node.Columns.String())
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
		constraintName = nodeStringsConcat("constraint", node.Name.String())
	}

	return nodeStringsConcat(constraintName, "unique", node.Columns.String())
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
		constraintName = nodeStringsConcat("constraint", node.Name.String())
	}

	return nodeStringsConcat(constraintName, "check(", node.Expr.String(), ")")
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
		returning = nodeStringsConcat("returning", node.ReturningClause.String())
	}

	if node.Select != nil {
		return nodeStringsConcat(
			"insert into",
			node.Table.Name.String(),
			node.Columns.String(),
			node.Select.String(),
			node.Upsert.String(),
			returning)
	}

	if node.DefaultValues {
		return nodeStringsConcat(
			"insert into",
			node.Table.Name.String(),
			"default values",
			returning,
		)
	}

	var rows []string
	for _, row := range node.Rows {
		rows = append(rows, row.String())
	}
	return nodeStringsConcat("insert into",
		node.Table.String(),
		node.Columns.String(),
		"values",
		strings.Join(rows, ","),
		node.Upsert.String(),
		returning,
	)
}

// Resolve returns a string representation with custom function nodes resolved to the values
// passed by resolver.
func (node *Insert) Resolve(resolver WriteStatementResolver) (string, error) {
	return resolveWriteStatementWalk(node, resolver)
}

func (node *Insert) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}

	if err := Walk(visit, node.Table, node.Columns, node.Upsert, node.Select); err != nil {
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
		clauses = append(clauses, nodeStringsConcat("on conflict", clause.String()))
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

// OnConflictClause represents an ON CONFLICT clause for upserts.
type OnConflictClause struct {
	Target   *OnConflictTarget
	DoUpdate *OnConflictUpdate
}

func (node *OnConflictClause) String() string {
	var target string
	if node.Target != nil {
		target = nodeStringsConcat(node.Target.Columns.String(), node.Target.Where.String())
	}

	if node.DoUpdate == nil {
		return nodeStringsConcat(target, "do nothing")
	}

	return nodeStringsConcat(target, "do update set", node.DoUpdate.Exprs.String(), node.DoUpdate.Where.String())
}

func (node *OnConflictClause) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}

	nodes := []Node{}
	if node.Target != nil {
		nodes = append(nodes, node.Target.Columns, node.Target.Where)
	}

	if node.DoUpdate != nil {
		nodes = append(nodes, node.DoUpdate.Exprs, node.DoUpdate.Where)
	}

	return Walk(visit, nodes...)
}

// OnConflictTarget represents an ON CONFLICT target for upserts.
type OnConflictTarget struct {
	Columns ColumnList
	Where   *Where
}

// OnConflictUpdate represents an ON CONFLICT.
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
	return nodeStringsConcat("delete from", node.Table.String(), node.Where.String())
}

// GetTable returns the table.
func (node *Delete) GetTable() *Table {
	return node.Table
}

// Resolve returns a string representation with custom function nodes resolved to the values
// passed by resolver.
func (node *Delete) Resolve(resolver WriteStatementResolver) (string, error) {
	return resolveWriteStatementWalk(node, resolver)
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
		returning = nodeStringsConcat("returning", node.ReturningClause.String())
	}

	return nodeStringsConcat("update", node.Table.String(), "set", node.Exprs.String(), node.Where.String(), returning)
}

// GetTable returns the table.
func (node *Update) GetTable() *Table {
	return node.Table
}

// Resolve returns a string representation with custom function nodes resolved to the values
// passed by resolver.
func (node *Update) Resolve(resolver WriteStatementResolver) (string, error) {
	return resolveWriteStatementWalk(node, resolver)
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

// UpdateExprs represents a slice of UpdateExpr.
type UpdateExprs []*UpdateExpr

// String returns the string representation of the node.
func (node UpdateExprs) String() string {
	var exprs []string
	for _, expr := range node {
		exprs = append(exprs, nodeStringsConcat(expr.Column.String(), "=", expr.Expr.String()))
	}

	return strings.Join(exprs, ",")
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
	return nodeStringsConcat("grant",
		node.Privileges.String(),
		"on",
		node.Table.String(),
		"to",
		"'"+strings.Join(node.Roles, "', '")+"'",
	)
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
	return strings.Join(privileges, ",")
}

// Len returns the length of privileges slice.
func (node Privileges) Len() int {
	return len(node)
}

func (node Privileges) walkSubtree(_ Visit) error {
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
	return nodeStringsConcat(
		"revoke",
		node.Privileges.String(),
		"on",
		node.Table.String(),
		"from",
		"'"+strings.Join(node.Roles, "', '")+"'",
	)
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

// AlterTableClause represents an ALTER TABLE operation such as RENAME, ADD, or DROP.
type AlterTableClause interface {
	Node
	iAlterTableClause()
}

func (*AlterTableRename) iAlterTableClause() {}
func (*AlterTableDrop) iAlterTableClause()   {}
func (*AlterTableAdd) iAlterTableClause()    {}

// AlterTable represents an ALTER TABLE statement.
type AlterTable struct {
	Table            *Table
	AlterTableClause AlterTableClause
}

// String returns the string representation of the node.
func (node *AlterTable) String() string {
	return fmt.Sprintf("alter table %s %s", node.Table.String(), node.AlterTableClause.String())
}

func (node *AlterTable) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}

	return Walk(visit, node.Table, node.AlterTableClause)
}

// GetTable returns the table that ALTER refers to.
func (node *AlterTable) GetTable() *Table {
	return node.Table
}

// Resolve returns a string representation with custom function nodes resolved to the values
// passed by resolver.
func (node *AlterTable) Resolve(resolver WriteStatementResolver) (string, error) {
	return resolveWriteStatementWalk(node, resolver)
}

// AlterTableRename represents the alter table clause that renames a column.
type AlterTableRename struct {
	OldColumn *Column
	NewColumn *Column
}

// String returns the string representation of the node.
func (node *AlterTableRename) String() string {
	return fmt.Sprintf("rename %s to %s", node.OldColumn.String(), node.NewColumn.String())
}

func (node *AlterTableRename) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}

	return Walk(visit, node.OldColumn, node.NewColumn)
}

// AlterTableDrop represents the alter table clause that drops a column.
type AlterTableDrop struct {
	Column *Column
}

// String returns the string representation of the node.
func (node *AlterTableDrop) String() string {
	return fmt.Sprintf("drop %s", node.Column.String())
}

func (node *AlterTableDrop) walkSubtree(visit Visit) error {
	if node != nil {
		return nil
	}

	return Walk(visit, node.Column)
}

// AlterTableAdd represents the alter table clause that adds a column.
type AlterTableAdd struct {
	ColumnDef *ColumnDef
}

// String returns the string representation of the node.
func (node *AlterTableAdd) String() string {
	return fmt.Sprintf("add %s", node.ColumnDef.String())
}

func (node *AlterTableAdd) walkSubtree(visit Visit) error {
	if node != nil {
		return nil
	}

	return Walk(visit, node.ColumnDef)
}

// resolvers

func resolveReadStatementWalk(node Node, resolver ReadStatementResolver) (string, error) {
	if resolver == nil {
		return "", errors.New("read resolver is needed")
	}

	resolveReadStatementParam := resolveReadStatementParam(resolver)
	err := Walk(func(node Node) (bool, error) {
		if funcExpr, ok := node.(*CustomFuncExpr); ok && funcExpr != nil {
			resolvedString, err := resolveReadStatementCustomFunc(funcExpr, resolver)
			if err != nil {
				return true, fmt.Errorf("resolve read statement: %s", err)
			}
			funcExpr.ResolvedString = resolvedString
		}

		if paramNode, ok := node.(*Param); ok {
			resolvedString, err := resolveReadStatementParam()
			if err != nil {
				return true, fmt.Errorf("resolve read statement: %s", err)
			}
			paramNode.ResolvedString = resolvedString
		}
		return false, nil
	}, node)
	if err != nil {
		return "", fmt.Errorf("failed to resolve while walking: %s", err)
	}
	return node.String(), nil
}

func resolveReadStatementCustomFunc(node *CustomFuncExpr, resolver ReadStatementResolver) (string, error) {
	switch node.Name {
	case "block_num":
		if len(node.Args) != 1 {
			return "", errors.New("block_num function should have exactly one argument")
		}

		value, ok := node.Args[0].(*Value)
		if !ok {
			return "", errors.New("argument of block_num is not a literal value")
		}

		if value.Type != IntValue {
			return "", errors.New("argument of block_num is not an integer")
		}

		chainID, err := strconv.ParseInt(string(value.Value), 10, 64)
		if err != nil {
			return "", fmt.Errorf("parsing argument to int: %s", err)
		}
		blockNumber, exists := resolver.GetBlockNumber(chainID)
		if !exists {
			return "", errors.New("chain id does not exist")
		}

		valueNode := &Value{Type: IntValue, Value: []byte(strconv.Itoa(int(blockNumber)))}
		return valueNode.String(), nil
	}

	return "", fmt.Errorf("custom function %s is not resolvable", node.Name)
}

// resolveReadStatementParam returns a function that acts like an iterator.
// Every time the function is called it gets the next bind value.
func resolveReadStatementParam(resolver ReadStatementResolver) func() (string, error) {
	bindValues := resolver.GetBindValues()
	i := 0

	return func() (string, error) {
		if i >= len(bindValues) {
			return "", fmt.Errorf("number of params is greater than the number of bind values")
		}

		s := bindValues[i].String()
		i++

		return s, nil
	}
}

func resolveWriteStatementWalk(node Node, resolver WriteStatementResolver) (string, error) {
	err := Walk(func(node Node) (bool, error) {
		if funcExpr, ok := node.(*CustomFuncExpr); ok && funcExpr != nil {
			resolvedString, err := resolveWriteStatement(funcExpr, resolver)
			if err != nil {
				return true, fmt.Errorf("resolve write statement: %s", err)
			}
			funcExpr.ResolvedString = resolvedString
		}
		return false, nil
	}, node)
	if err != nil {
		return "", fmt.Errorf("failed to resolve while walking: %s", err)
	}
	return node.String(), nil
}

func resolveWriteStatement(node *CustomFuncExpr, resolver WriteStatementResolver) (string, error) {
	if resolver == nil {
		return "", errors.New("write resolver is needed")
	}

	switch node.Name {
	case "block_num":
		if node.Args == nil {
			return "", errors.New("block_num arguments cannot be nil")
		}

		if len(node.Args) != 0 {
			return "", errors.New("block_num function should have exactly zero arguments")
		}

		blockNumber := resolver.GetBlockNumber()
		valueNode := &Value{Type: IntValue, Value: []byte(strconv.Itoa(int(blockNumber)))}
		return valueNode.String(), nil
	case "txn_hash":
		if node.Args == nil {
			return "", errors.New("txn_hash arguments cannot be nil")
		}

		if len(node.Args) != 0 {
			return "", errors.New("txn_hash function should have exactly zero arguments")
		}

		txnHash := resolver.GetTxnHash()
		valueNode := &Value{Type: StrValue, Value: []byte(txnHash)}
		return valueNode.String(), nil
	}

	return "", fmt.Errorf("custom function %s is not resolvable", node.Name)
}
