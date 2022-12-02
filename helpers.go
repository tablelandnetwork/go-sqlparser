package sqlparser

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
		if table, ok := node.(*Table); ok && table != nil {
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
