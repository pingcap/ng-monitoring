package docdb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSQLite(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), "ngm-test-.*")
	require.NoError(t, err)
	db, err := NewSQLiteDB(dir, true)
	require.NoError(t, err)
	testDocDB(t, db)
}
