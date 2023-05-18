package sqlparser

// AllowedFunctions is a map of allowed functions in Tableland.
// The value indicates if the function is custom.
var AllowedFunctions = map[string]bool{
	// core functions
	"abs": false,
	//"changes":                   false,
	"char":     false,
	"coalesce": false,
	"format":   false,
	"glob":     false,
	"hex":      false,
	"ifnull":   false,
	"iif":      false,
	"instr":    false,
	//"last_insert_rowid":         false,
	"length": false,
	"like":   false,
	//"likelihood":                false,
	//"likely":                    false,
	//"load_extension":            false,
	"lower":  false,
	"ltrim":  false,
	"max":    false,
	"min":    false,
	"nullif": false,
	"printf": false,
	"quote":  false,
	//"random":                    false,
	//"randomblob":                false,
	"replace": false,
	"round":   false,
	"rtrim":   false,
	"sign":    false,
	//"soundex":                   false,
	//"sqlite_compileoption_get":  false,
	//"sqlite_compileoption_used": false,
	//"sqlite_offset":             false,
	//"sqlite_source_id":          false,
	//"sqlite_version":            false,
	"substr":    false,
	"substring": false,
	//"total_changes": false,
	"trim":    false,
	"typeof":  false,
	"unicode": false,
	//"unlikely": false,
	"upper": false,
	//"zeroblob": false,

	// math functions
	"acos":    false,
	"acosh":   false,
	"asin":    false,
	"asinh":   false,
	"atan":    false,
	"atan2":   false,
	"atanh":   false,
	"ceil":    false,
	"ceiling": false,
	"cos":     false,
	"cosh":    false,
	"degrees": false,
	"exp":     false,
	"floor":   false,
	"ln":      false,
	"log":     false,
	"log10":   false,
	"log2":    false,
	"mod":     false,
	"pi":      false,
	"pow":     false,
	"power":   false,
	"radians": false,
	"sin":     false,
	"sinh":    false,
	"sqrt":    false,
	"tan":     false,
	"tanh":    false,
	"trunc":   false,

	// date & time functions
	// "date":      false,
	// "time":      false,
	// "datetime":  false,
	// "julianday": false,
	// "unixepoch": false,
	// "strftime":  false,

	// json functions
	"json":              false,
	"json_array":        false,
	"json_array_length": false,
	"json_extract":      false,
	"json_insert":       false,
	"json_object":       false,
	"json_patch":        false,
	"json_remove":       false,
	"json_replace":      false,
	"json_set":          false,
	"json_type":         false,
	"json_valid":        false,
	"json_quote":        false,
	"json_group_array":  false,
	"json_group_object": false,

	// aggregate functions
	"avg":          false,
	"count":        false,
	"group_concat": false,
	// "max":          false,
	// "min":          false,
	"sum":   false,
	"total": false,

	// custom Tableland functions
	"txn_hash":  true,
	"block_num": true,
}
