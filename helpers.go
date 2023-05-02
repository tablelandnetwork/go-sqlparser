package sqlparser

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Visit defines the signature of a function that
// can be used to visit all nodes of a parse tree.
type Visit func(node Node) (stop bool, err error)

// Walk calls visit on every node.
// If visit returns false, the underlying nodes
// are also visited. If it returns an error, walking
// is interrupted, and the error is returned.
func Walk(visit Visit, nodes ...Node) error {
	for _, node := range nodes {
		if node == nil {
			continue
		}
		stop, err := visit(node)
		if err != nil {
			return err
		}
		if !stop {
			if err = node.walkSubtree(visit); err != nil {
				return err
			}
		}
	}
	return nil
}

// GetUniqueTableReferences returns a slice of tables' names referenced by the node.
func GetUniqueTableReferences(node Node) []string {
	if node == nil {
		return []string{}
	}

	tables := map[string]struct{}{}
	tableNames := []string{}

	// it's ok to ignore the error because the visit function does not throw an error
	_ = Walk(func(node Node) (bool, error) {
		if table, ok := node.(*Table); ok && table != nil && table.IsTarget {
			tableName := table.Name.String()
			if _, ok := tables[tableName]; !ok {
				tables[tableName] = struct{}{}
				tableNames = append(tableNames, tableName)
			}
		}
		return false, nil
	}, node)

	return tableNames
}

// ValidateTargetTables recursively validates all tables found in the node and return them.
func ValidateTargetTables(node Node) ([]*ValidatedTable, error) {
	if node == nil {
		return []*ValidatedTable{}, nil
	}

	tables := map[string]struct{}{}
	validTables := []*ValidatedTable{}
	err := Walk(func(node Node) (bool, error) {
		if table, ok := node.(*Table); ok && table != nil && table.IsTarget {
			tables[table.String()] = struct{}{}
			validTable, err := ValidateTargetTable(table)
			if err != nil {
				return true, fmt.Errorf("validate: %s", err)
			}
			validTables = append(validTables, validTable)
		}
		return false, nil
	}, node)
	if err != nil {
		return []*ValidatedTable{}, fmt.Errorf("walk subtree: %s", err)
	}

	return validTables, nil
}

var (
	tableNameRegEx       = regexp.MustCompile("^([A-Za-z]+[A-Za-z0-9_]*)*_[0-9]+_[0-9]+$")
	createTableNameRegEx = regexp.MustCompile("^([A-Za-z]+[A-Za-z0-9_]*)*_[0-9]+$")
)

func ValidateTargetTable(table *Table) (*ValidatedTable, error) {
	if !table.IsTarget {
		return nil, fmt.Errorf("table is not target")
	}

	closingChar := map[byte]byte{
		'"': '"',
		'`': '`',
		'[': ']',
	}

	tableName := table.String()
	for start, end := range closingChar {
		if tableName[0] == start && tableName[len(tableName)-1] == end {
			tableName = tableName[1 : len(tableName)-1]
			break
		}
	}

	if !tableNameRegEx.MatchString(tableName) {
		return nil, &ErrTableNameWrongFormat{Name: tableName}
	}

	parts := strings.Split(tableName, "_")
	if len(parts) < 3 {
		return nil, fmt.Errorf("not enough parts in the name")
	}

	prefix := strings.Join(parts[:len(parts)-2], "_")
	chainIDstr := parts[len(parts)-2]
	tokenIDstr := parts[len(parts)-1]

	chainID, err := strconv.ParseInt(chainIDstr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parsing chain id in table name: %s", err)
	}

	tokenID, err := strconv.ParseInt(tokenIDstr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parsing token id in table name: %s", err)
	}

	return &ValidatedTable{name: table.String(), prefix: prefix, chainID: chainID, tokenID: tokenID}, nil
}

func ValidateCreateTargetTable(table *Table) (*ValidatedCreateTable, error) {
	if !table.IsTarget {
		return nil, fmt.Errorf("table is not target")
	}

	closingChar := map[byte]byte{
		'"': '"',
		'`': '`',
		'[': ']',
	}

	tableName := table.String()
	for start, end := range closingChar {
		if tableName[0] == start && tableName[len(tableName)-1] == end {
			tableName = tableName[1 : len(tableName)-1]
			break
		}
	}

	if !createTableNameRegEx.MatchString(tableName) {
		return nil, &ErrTableNameWrongFormat{Name: tableName}
	}

	parts := strings.Split(tableName, "_")
	if len(parts) < 2 {
		return nil, fmt.Errorf("not enough parts in the name")
	}

	prefix := strings.Join(parts[:len(parts)-1], "_")
	chainIDstr := parts[len(parts)-1]

	chainID, err := strconv.ParseInt(chainIDstr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parsing chain id in table name: %s", err)
	}

	return &ValidatedCreateTable{name: table.String(), prefix: prefix, chainID: chainID}, nil
}

// containsSubquery checks recursively if the node contains a subquery.
func containsSubquery(node Node) bool {
	if node == nil {
		return false
	}
	var containsSubquery bool

	// it's ok to ignore the error because the visit function does not throw an error
	_ = Walk(func(node Node) (bool, error) {
		if _, ok := node.(*Subquery); ok {
			containsSubquery = true
			return true, nil
		}
		return false, nil
	}, node)

	return containsSubquery
}

// ValidatedTable is a Table that was validated by ValidateTargetTable.
type ValidatedTable struct {
	name    string
	prefix  string
	chainID int64
	tokenID int64
}

// Name returns the table's name.
func (node *ValidatedTable) Name() string {
	return node.name
}

// ChainID returns the table's chain id.
func (node *ValidatedTable) ChainID() int64 {
	return node.chainID
}

// TokenID returns the table's token id.
func (node *ValidatedTable) TokenID() int64 {
	return node.tokenID
}

// Prefix returns table's prefix.
func (node *ValidatedTable) Prefix() string {
	return node.prefix
}

// ValidatedCreateTable is a Table that was validated by ValidateCreateTargetTable.
type ValidatedCreateTable struct {
	name    string
	prefix  string
	chainID int64
}

// Name returns the table's name.
func (node *ValidatedCreateTable) Name() string {
	return node.name
}

// ChainID returns the table's chain id.
func (node *ValidatedCreateTable) ChainID() int64 {
	return node.chainID
}

// Prefix returns table's prefix.
func (node *ValidatedCreateTable) Prefix() string {
	return node.prefix
}
