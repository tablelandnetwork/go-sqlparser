package sqlparser

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetUniqueTableReferences(t *testing.T) {
	t.Parallel()

	t.Run("nil", func(t *testing.T) {
		t.Parallel()
		require.ElementsMatch(t, []string{}, GetUniqueTableReferences(nil))
	})

	t.Run("select", func(t *testing.T) {
		t.Parallel()

		sql := "SELECT t.id, t3.* FROM t, t2 JOIN t3 JOIN (SELECT * FROM t4);"
		ast, err := Parse(sql)
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"t", "t3", "t2", "t4"}, GetUniqueTableReferences(ast))
	})
}

func TestValidateTargetTable(t *testing.T) {
	t.Parallel()

	invalidWrongFormatTests := []string{
		"t",
		"t2",
		"t_2_",
		"t_",
		"__",
		"t__",
		"t_2__",
		"__1",
	}

	for _, name := range invalidWrongFormatTests {
		func(name string) {
			t.Run(fmt.Sprintf("invalid wrong format:%s", name), func(t *testing.T) {
				t.Parallel()
				table := &Table{Name: Identifier(name), IsTarget: true}
				validTable, err := ValidateTargetTable(table)
				require.Error(t, err)

				e := &ErrTableNameWrongFormat{Name: name}
				require.ErrorAs(t, err, &e)
				require.Nil(t, validTable)
			})
		}(name)
	}

	t.Run("valid table name", func(t *testing.T) {
		table := &Table{Name: Identifier("t_1_2"), IsTarget: true}
		validTable, err := ValidateTargetTable(table)
		require.NoError(t, err)
		require.Equal(t, "t", validTable.Prefix())
		require.Equal(t, int64(1), validTable.ChainID())
		require.Equal(t, int64(2), validTable.TokenID())
		require.Equal(t, "t_1_2", validTable.Name())
	})

	t.Run("valid table name enclosing", func(t *testing.T) {
		table := &Table{Name: Identifier("[t_1_2]"), IsTarget: true}
		validTable, err := ValidateTargetTable(table)
		require.NoError(t, err)
		require.Equal(t, "t", validTable.Prefix())
		require.Equal(t, int64(1), validTable.ChainID())
		require.Equal(t, int64(2), validTable.TokenID())
		require.Equal(t, "[t_1_2]", validTable.Name())
	})

	t.Run("valid table name multple char", func(t *testing.T) {
		table := &Table{Name: Identifier("table_1_2"), IsTarget: true}
		validTable, err := ValidateTargetTable(table)
		require.NoError(t, err)
		require.Equal(t, "table", validTable.Prefix())
		require.Equal(t, int64(1), validTable.ChainID())
		require.Equal(t, int64(2), validTable.TokenID())
		require.Equal(t, "table_1_2", validTable.Name())
	})

	t.Run("valid table name without prefix", func(t *testing.T) {
		table := &Table{Name: Identifier("_1_2"), IsTarget: true}
		validTable, err := ValidateTargetTable(table)
		require.NoError(t, err)
		require.Equal(t, "", validTable.Prefix())
		require.Equal(t, int64(1), validTable.ChainID())
		require.Equal(t, int64(2), validTable.TokenID())
		require.Equal(t, "_1_2", validTable.Name())
	})

	t.Run("valid create table name", func(t *testing.T) {
		table := &Table{Name: Identifier("t_1"), IsTarget: true}
		validTable, err := ValidateCreateTargetTable(table)
		require.NoError(t, err)
		require.Equal(t, "t", validTable.Prefix())
		require.Equal(t, int64(1), validTable.ChainID())
		require.Equal(t, "t_1", validTable.Name())
	})

	t.Run("valid create table name enclosing", func(t *testing.T) {
		table := &Table{Name: Identifier("[t_1]"), IsTarget: true}
		validTable, err := ValidateCreateTargetTable(table)
		require.NoError(t, err)
		require.Equal(t, "t", validTable.Prefix())
		require.Equal(t, int64(1), validTable.ChainID())
		require.Equal(t, "[t_1]", validTable.Name())
	})

	t.Run("valid create table name without prefix", func(t *testing.T) {
		table := &Table{Name: Identifier("_1"), IsTarget: true}
		validTable, err := ValidateCreateTargetTable(table)
		require.NoError(t, err)
		require.Equal(t, "", validTable.Prefix())
		require.Equal(t, int64(1), validTable.ChainID())
		require.Equal(t, "_1", validTable.Name())
	})

	t.Run("valid create table name with consecutive underscore", func(t *testing.T) {
		table := &Table{Name: Identifier("t_2_1__1"), IsTarget: true}
		validTable, err := ValidateCreateTargetTable(table)
		require.NoError(t, err)
		require.Equal(t, "t_2_1_", validTable.Prefix())
		require.Equal(t, int64(1), validTable.ChainID())
		require.Equal(t, "t_2_1__1", validTable.Name())
	})
}

func TestWalk(t *testing.T) {
	t.Parallel()
	t.Run("upsert", func(t *testing.T) {
		t.Parallel()

		sql := "insert into test_31337_2 values (1,'a') on conflict do update set val='new';"
		ast, err := Parse(sql)
		require.NoError(t, err)

		err = Walk(func(node Node) (stop bool, err error) {
			return false, nil
		}, ast)
		require.NoError(t, err)
	})
}
