package sqlparser

const (
	// MaxTextLength is the limit for the length of a TEXT literal value.
	MaxTextLength = 1024
	// MaxBlobLength is the limit for the length of a BLOB literal value.
	MaxBlobLength = 1024
	// MaxAllowedColumns is the limit for the number of columns in a CREATE TABLE statement.
	MaxAllowedColumns = 24
)
