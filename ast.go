package sqlparser

import (
	"fmt"
	"strings"

	"github.com/davecgh/go-spew/spew"
)

// Node represents a node in the AST.
type Node interface {
	String() string
}

// AST represents the root Node of the AST.
type AST struct {
	Root Statement
}

func (node *AST) String() string {
	if node.Root == nil {
		return ""
	}
	return node.Root.String()
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

func (*Select) iStatement()      {}
func (*CreateTable) iStatement() {}

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

// String returns the string representation of the node.
func (node TableExprList) String() string {
	if len(node) == 0 {
		return ""
	}
	var strs []string
	for _, e := range node {
		strs = append(strs, e.String())
	}

	return strings.Join(strs, ", ")
}

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

// String returns the string representation of the node.
func (node *Subquery) String() string {
	return fmt.Sprintf("(%s)", node.Select.String())
}

// ParenTableExpr represents a parenthesized list of TableExpr.
type ParenTableExpr struct {
	TableExprList TableExprList
}

// String returns the string representation of the node.
func (node *ParenTableExpr) String() string {
	return fmt.Sprintf("(%v)", node.TableExprList.String())
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

// String returns the string representation of the node.
func (node *JoinTableExpr) String() string {
	if node.On != nil {
		return fmt.Sprintf("%s %s %s on %s", node.LeftExpr.String(), node.JoinOperator, node.RightExpr.String(), node.On.String())
	}

	if node.Using != nil {
		return fmt.Sprintf("%s %s %s using %s", node.LeftExpr.String(), node.JoinOperator, node.RightExpr.String(), node.Using.String())
	}

	return fmt.Sprintf("%s %s %s", node.LeftExpr.String(), node.JoinOperator, node.RightExpr.String())
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

// String returns the string representation of the node.
func (node *NullValue) String() string {
	return "null"
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

// IsExpr represents a IS expression
type IsExpr struct {
	Left, Right Expr
}

// String returns the string representation of the node.
func (node *IsExpr) String() string {
	return fmt.Sprintf("%s is %s", node.Left.String(), node.Right.String())
}

// IsNullExpr represents a IS expression
type IsNullExpr struct {
	Expr Expr
}

// String returns the string representation of the node.
func (node *IsNullExpr) String() string {
	return fmt.Sprintf("%s isnull", node.Expr.String())
}

// NotNullExpr represents a IS expression
type NotNullExpr struct {
	Expr Expr
}

// String returns the string representation of the node.
func (node *NotNullExpr) String() string {
	return fmt.Sprintf("%s notnull", node.Expr.String())
}

// CollateExpr the COLLATE operator
type CollateExpr struct {
	Expr          Expr
	CollationName string
}

// String returns the string representation of the node.
func (node *CollateExpr) String() string {
	return fmt.Sprintf("%s collate %s", node.Expr.String(), node.CollationName)
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

// String returns the string representation of the node.
func (node *ConvertExpr) String() string {
	return fmt.Sprintf("cast (%s as %s)", node.Expr.String(), string(node.Type))
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

// Table represents a table.
type Table struct {
	Name Identifier
}

// String returns the string representation of the node.
func (node *Table) String() string {
	return node.Name.String()
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

// ColumnList is a list of columns.
type ColumnList []*Column

// String returns the string representation of the node.
func (node ColumnList) String() string {
	var strs []string
	for _, col := range node {
		strs = append(strs, col.String())
	}

	return fmt.Sprintf("%s%s%s", "(", strings.Join(strs, ", "), ")")
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

// ExistsExpr represents a EXISTS expression.
type ExistsExpr struct {
	Subquery *Subquery
}

// String returns the string representation of the node.
func (node *ExistsExpr) String() string {
	return fmt.Sprintf("exists %s", node.Subquery.String())
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

// Identifier represents a Column, Table and Function name identifier.
type Identifier string

// String returns the string representation of the node.
func (node Identifier) String() string {
	return string(node)
}

// IsEmpty returns if the identifier is empty.
func (node Identifier) IsEmpty() bool {
	return node == ""
}

// CreateTable represents a CREATE TABLE statement.
type CreateTable struct {
	Name        *Table
	Columns     []*ColumnDef
	Constraints []*TableConstraint

	// This is the only TableOption supported in the AST.
	// The grammar cannot parse this option.
	// It is used to toggle the strict mode directiy in the AST.
	StrictMode bool
}

// String returns the string representation of the node.
func (node *CreateTable) String() string {
	columns := []string{}
	for _, column := range node.Columns {
		columns = append(columns, column.String())
	}
	column := strings.Join(columns, ", ")
	if len(node.Constraints) > 0 {
		constraints := []string{}
		for _, constraint := range node.Constraints {
			constraints = append(constraints, constraint.String())
		}
		column += "," + strings.Join(constraints, ",")
	}

	if node.StrictMode {
		return fmt.Sprintf("CREATE TABLE %s (%s) strict", node.Name.String(), column)
	} else {
		return fmt.Sprintf("CREATE TABLE %s (%s)", node.Name.String(), column)
	}
}

// ColumnDef represents the column definition of a CREATE TABLE statement.
type ColumnDef struct {
	Name        *Column
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
	return fmt.Sprintf("%s %s%s", node.Name, node.Type, constraint)
}

// Types for ColumnDef type.
const (
	TypeIntStr     = "INT"
	TypeIntegerStr = "INTEGER"
	TypeRealStr    = "REAL"
	TypeTextStr    = "TEXT"
	TypeBlobStr    = "BLOB"
	TypeAnyStr     = "ANY"
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

// func (*ColumnConstraintGenerated) iColumnConstraint() {}

type ColumnConstraintPrimaryKey struct {
	Order string
	//ConflictClause *ConflictClause
}

// String returns the string representation of the node.
func (node *ColumnConstraintPrimaryKey) String() string {
	if node.Order == ColumnConstraintPrimaryKeyOrderEmpty {
		return "PRIMARY KEY"
	}
	return fmt.Sprintf("PRIMARY KEY %s", node.Order)
}

const (
	ColumnConstraintPrimaryKeyOrderEmpty = ""
	ColumnConstraintPrimaryKeyOrderAsc   = "ASC"
	ColumnConstraintPrimaryKeyOrderDesc  = "DESC"
)

type ColumnConstraintNotNull struct {
	//ConflictClause *ConflictClause
}

// String returns the string representation of the node.
func (node *ColumnConstraintNotNull) String() string {
	return "NOT NULL"
}

type ColumnConstraintUnique struct {
	//ConflictClause *ConflictClause
}

// String returns the string representation of the node.
func (node *ColumnConstraintUnique) String() string {
	return "UNIQUE"
}

type ColumnConstraintCheck struct {
	Expr Expr
}

// String returns the string representation of the node.
func (node *ColumnConstraintCheck) String() string {
	return fmt.Sprintf("CHECK(%s)", node.Expr.String())
}

type ColumnConstraintDefault struct {
	Expr        Expr
	Parenthesis bool
}

// String returns the string representation of the node.
func (node *ColumnConstraintDefault) String() string {
	if node.Parenthesis {
		return fmt.Sprintf("DEFAULT (%s)", node.Expr.String())
	}
	return fmt.Sprintf("DEFAULT %s", node.Expr.String())
}

// TableConstraint is constraint for table definition.
type TableConstraint struct {
	Type TableConstraintType
	Name string
	// Used for PRIMARY KEY, UNIQUE, ......
	Keys []Identifier
}

func (node TableConstraint) String() string {
	keys := []string{}
	for _, key := range node.Keys {
		keys = append(keys, fmt.Sprintf("`%v`", key))
	}
	name := ""
	if node.Name != "" {
		name = fmt.Sprintf("`%s`", node.Name)
	}
	return fmt.Sprintf("%s %s (%s)", node.Type.String(), name, strings.Join(keys, ", "))
}

type TableConstraintType int

const (
	TableConstraintNoTableConstraint TableConstraintType = iota
	TableConstraintPrimaryKey
	TableConstraintUniq
	TableConstraintCheck
)

func (t TableConstraintType) String() string {
	switch t {
	case TableConstraintPrimaryKey:
		return "PRIMARY KEY"
	case TableConstraintUniq:
		return "UNIQUE"
	case TableConstraintCheck:
		return "CHECK"
	}
	return ""
}
