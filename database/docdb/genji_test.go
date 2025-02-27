package docdb

import (
	"os"
	"testing"
	"time"

	"github.com/pingcap/ng-monitoring/utils/testutil"

	"github.com/stretchr/testify/require"
)

func TestGenji(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), "ngm-test-.*")
	require.NoError(t, err)
	db, err := NewGenjiDBFromGenji(testutil.NewGenjiDB(t, dir))
	require.NoError(t, err)
	testDocDB(t, db)
}

func TestGC(t *testing.T) {
	tmpDir, err := os.MkdirTemp(os.TempDir(), "ngm-test-.*")
	require.NoError(t, err)
	defer func() {
		err := os.RemoveAll(tmpDir)
		require.NoError(t, err)
	}()

	db := testutil.NewBadgerDB(t, tmpDir)
	ts, err := getLastFlattenTs(db)
	require.NoError(t, err)
	require.Equal(t, int64(0), ts)

	ts = time.Now().Unix()
	err = storeLastFlattenTs(db, ts)
	require.NoError(t, err)

	lastTs, err := getLastFlattenTs(db)
	require.NoError(t, err)
	require.Equal(t, ts, lastTs)

	require.False(t, needFlatten(db))
	runGC(db)

	lastTs = ts - int64(flattenInterval/time.Second)
	err = storeLastFlattenTs(db, lastTs)
	require.NoError(t, err)
	require.True(t, needFlatten(db))

	runGC(db)
	lastFlattenTs, err := getLastFlattenTs(db)
	require.NoError(t, err)
	require.NotEqual(t, lastTs, lastFlattenTs)
	require.Less(t, time.Now().Unix()-lastFlattenTs, int64(10))
}
