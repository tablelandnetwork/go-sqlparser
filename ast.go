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
	Root Node
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

// Value represents a sing.
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
	Name     string
	TableRef *Table
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
	return c.Name
}

// Exprs represents a list of expressions.
type Exprs []Expr

// ToString returns the string representation of the node.
func (c Exprs) ToString() string {
	var strs []string
	for _, e := range c {
		strs = append(strs, e.ToString()) // note the = instead of :=
	}

	return fmt.Sprintf("%s%s%s", "(", strings.Join(strs, ", "), ")")
}
