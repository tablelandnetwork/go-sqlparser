package sqlparser

// Visit defines the signature of a function that
// can be used to visit all nodes of a parse tree.
type Visit func(node Node) (kontinue bool, err error)

// Walk calls visit on every node.
// If visit returns true, the underlying nodes
// are also visited. If it returns an error, walking
// is interrupted, and the error is returned.
func Walk(visit Visit, nodes ...Node) error {
	for _, node := range nodes {
		if node == nil {
			continue
		}
		kontinue, err := visit(node)
		if err != nil {
			return err
		}
		if kontinue {
			err = node.walkSubtree(visit)
			if err != nil {
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
	if table, ok := node.(*Table); ok && table != nil {
		tables[table.Name.String()] = struct{}{}
		tableNames = append(tableNames, table.Name.String())
	}

	// it's ok to ignore the error because the visit function does not throw an error
	_ = node.walkSubtree(func(node Node) (bool, error) {
		if table, ok := node.(*Table); ok && table != nil {
			tableName := table.Name.String()
			if _, ok := tables[tableName]; !ok {
				tables[tableName] = struct{}{}
				tableNames = append(tableNames, tableName)
			}
		}
		return true, nil
	})

	return tableNames
}
