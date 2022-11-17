package sqlparser

import "fmt"

// ErrSyntaxError indicates a syntax error.
type ErrSyntaxError struct {
	YaccError string
	Position  int
	Literal   string
}

func (e *ErrSyntaxError) Error() string {
	return fmt.Sprintf("%s at position %d near '%s'", e.YaccError, e.Position, string(e.Literal))
}

// ErrKeywordIsNotAllowed indicates an error for keyword that is not allowed (eg CURRENT_TIME).
type ErrKeywordIsNotAllowed struct {
	Keyword string
}

func (e *ErrKeywordIsNotAllowed) Error() string {
	return fmt.Sprintf("keyword not allowed: %s", e.Keyword)
}

// ErrTextTooLong is an error returned when a query contains a
// text constant that is too long.
type ErrTextTooLong struct {
	Length     int
	MaxAllowed int
}

func (e *ErrTextTooLong) Error() string {
	return fmt.Sprintf("text field length is too long (has %d, max %d)",
		e.Length, e.MaxAllowed)
}

// ErrBlobTooBig is an error returned when a query contains a
// BLOB constant that is too long.
type ErrBlobTooBig struct {
	Length     int
	MaxAllowed int
}

func (e *ErrBlobTooBig) Error() string {
	return fmt.Sprintf("text field length is too long (has %d, max %d)",
		e.Length, e.MaxAllowed)
}

// ErrTooManyColumns is an error returned when a create statement has
// more columns that allowed.
type ErrTooManyColumns struct {
	ColumnCount int
	MaxAllowed  int
}

func (e *ErrTooManyColumns) Error() string {
	return fmt.Sprintf("table has too many columns (has %d, max %d)",
		e.ColumnCount, e.MaxAllowed)
}

// ErrStatementContainsSubquery indicates a statement contains a subquery.
type ErrStatementContainsSubquery struct {
	StatementKind string
}

func (e *ErrStatementContainsSubquery) Error() string {
	return fmt.Sprintf("%s contains subquery", e.StatementKind)
}

// ErrNoSuchFunction indicates that the function called does not exist.
type ErrNoSuchFunction struct {
	FunctionName string
}

func (e *ErrNoSuchFunction) Error() string {
	return fmt.Sprintf("no such: %s", e.FunctionName)
}

// ErrUpdateColumnsAndValuesDiffer indicates that there's a mismatch between the number of columns and number of values.
type ErrUpdateColumnsAndValuesDiffer struct {
	ColumnsCount int
	ValuesCount  int
}

func (e *ErrUpdateColumnsAndValuesDiffer) Error() string {
	return fmt.Sprintf("%d columns assigned %d values", e.ColumnsCount, e.ValuesCount)
}

// ErrGrantRepeatedPrivilege indicates a repeated privilege.
type ErrGrantRepeatedPrivilege struct {
	Privilege string
}

func (e *ErrGrantRepeatedPrivilege) Error() string {
	return fmt.Sprintf("repeated privilege: %s", e.Privilege)
}

// ErrMultiplePrimaryKey indicates a that a CREATE statement has more than one primary key.
type ErrMultiplePrimaryKey struct{}

func (e *ErrMultiplePrimaryKey) Error() string {
	return "has more than one primary key"
}

// ErrUpsertMissingTarget indicates a missing conflict target.
// The conflict target may be omitted on the last ON CONFLICT clause in the INSERT statement, but is required for all other ON CONFLICT clause.
type ErrUpsertMissingTarget struct{}

func (e *ErrUpsertMissingTarget) Error() string {
	return "has a missing conflict target"
}

// ErrRowIDNotAllowed indicates a reference to the columns rowid, _rowid_, or oid in an INSERT, UPDATE or CREATE statement.
type ErrRowIDNotAllowed struct{}

func (e *ErrRowIDNotAllowed) Error() string {
	return "rowid is not allowed"
}

// ErrNumericLiteralFloat indicates a literal numeric float is being used.
type ErrNumericLiteralFloat struct {
	Value []byte
}

func (e *ErrNumericLiteralFloat) Error() string {
	return "literal numeric floats are not allowed"
}
