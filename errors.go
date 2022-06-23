package sqlparser

import "fmt"

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
