package sqlparser

// containsSubquery checks recursively if the node contains a subquery.
func containsSubquery(node Node) bool {
	if node == nil {
		return false
	}

	if subquery, ok := node.(*Subquery); ok && subquery != nil {
		return true
	}

	var containsSubquery bool
	// it's ok to ignore the error because the visit function does not throw an error
	_ = node.walkSubtree(func(node Node) (bool, error) {
		if _, ok := node.(*Subquery); ok {
			containsSubquery = true
			return false, nil
		}
		return true, nil
	})

	return containsSubquery
}
