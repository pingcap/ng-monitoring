package store

import (
	"bytes"
	"context"
	"math/rand"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/pingcap/ng-monitoring/component/conprof/meta"
	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/ng-monitoring/database/docdb"
	"github.com/pingcap/ng-monitoring/utils/testutil"

	"github.com/dgraph-io/badger/v3"
	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/engine/badgerengine"
	"github.com/genjidb/genji/types"
	"github.com/stretchr/testify/require"
)

func TestProfileStorage(t *testing.T) {
	tmpDir, err := os.MkdirTemp(os.TempDir(), "ngm-test-.*")
	require.NoError(t, err)
	defer func() {
		err := os.RemoveAll(tmpDir)
		require.NoError(t, err)
	}()
	baseTs := time.Now().Unix()
	for i := 0; i < 2; i++ {
		testProfileStorage(t, tmpDir, baseTs+int64(i*1000), i > 0)
	}

	testProfileStorageGC(t, tmpDir, baseTs)
}

func testProfileStorageGC(t *testing.T, tmpDir string, baseTs int64) {
	db, err := docdb.NewGenjiDBFromGenji(testutil.NewGenjiDB(t, tmpDir))
	require.NoError(t, err)
	defer db.Close()
	storage, err := NewProfileStorage(db)
	require.NoError(t, err)
	defer storage.Close()

	originSize := len(storage.metaCache)
	require.True(t, originSize > 0)

	cfg := config.GetDefaultConfig()
	cfg.ContinueProfiling.DataRetentionSeconds = -100000
	config.StoreGlobalConfig(cfg)
	storage.runGC()
	require.Equal(t, 0, len(storage.metaCache))
}

func testProfileStorage(t *testing.T, tmpDir string, baseTs int64, cleanCache bool) {
	db, err := docdb.NewGenjiDBFromGenji(testutil.NewGenjiDB(t, tmpDir))
	require.NoError(t, err)
	defer db.Close()
	storage, err := NewProfileStorage(db)
	require.NoError(t, err)
	defer storage.Close()

	if cleanCache {
		storage.metaCache = make(map[meta.ProfileTarget]*meta.TargetInfo)
	}

	cases := []struct {
		kind      string
		component string
		address   string
		data      []byte
	}{
		{"profile", "tidb", "127.0.0.1:10080", mockProfile()},
		{"profile", "pd", "127.0.0.1:2379", mockProfile()},
		{"profile", "tikv", "127.0.0.1:20180", mockProfile()},
		{"goroutine", "tidb", "127.0.0.1:10080", mockProfile()},
		{"heap", "tidb", "127.0.0.1:10080", mockProfile()},
	}

	for i, ca := range cases {
		pt := meta.ProfileTarget{Kind: ca.kind, Component: ca.component, Address: ca.address}
		ts := baseTs + int64(i)
		err = storage.AddProfile(pt, time.Unix(ts, 0), ca.data, nil)
		require.NoError(t, err)

		param := &meta.BasicQueryParam{
			Begin:   ts,
			End:     ts,
			Limit:   100,
			Targets: []meta.ProfileTarget{pt},
		}
		list, err := storage.QueryGroupProfiles(param)
		require.NoError(t, err)
		require.Equal(t, 1, len(list))
		require.Equal(t, pt, list[0].Target)
		require.Equal(t, 1, len(list[0].TsList))
		require.Equal(t, ts, list[0].TsList[0])

		executed := false
		err = storage.QueryProfileData(param, func(target meta.ProfileTarget, timestamp int64, data []byte) error {
			require.Equal(t, pt, target)
			require.Equal(t, ts, timestamp)
			require.Equal(t, ca.data, data)
			executed = true
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, true, executed)

		_, err = storage.UpdateProfileTargetInfo(pt, ts)
		require.NoError(t, err)

		info := storage.GetTargetInfoFromCache(pt)
		require.NotNil(t, info)
		require.Equal(t, info.LastScrapeTs, ts)
	}
	param := &meta.BasicQueryParam{
		Begin: baseTs,
		End:   baseTs + int64(len(cases)),
		Limit: 100,
	}
	lists, err := storage.QueryGroupProfiles(param)
	require.NoError(t, err)
	require.Equal(t, len(cases), len(lists))
	for _, list := range lists {
		found := false
		for idx, ca := range cases {
			pt := meta.ProfileTarget{Kind: ca.kind, Component: ca.component, Address: ca.address}
			if pt == list.Target {
				require.Equal(t, 1, len(list.TsList))
				require.Equal(t, baseTs+int64(idx), list.TsList[0])
				found = true
			}
		}
		require.Equal(t, true, found)
	}

	err = storage.QueryProfileData(param, func(target meta.ProfileTarget, timestamp int64, data []byte) error {
		found := false
		for idx, ca := range cases {
			pt := meta.ProfileTarget{Kind: ca.kind, Component: ca.component, Address: ca.address}
			if pt == target {
				require.Equal(t, baseTs+int64(idx), timestamp)
				require.Equal(t, ca.data, data)
				found = true
			}
		}
		require.Equal(t, true, found)
		return nil
	})
	require.NoError(t, err)
}

func TestStoreProfileStatus(t *testing.T) {
	tmpDir, err := os.MkdirTemp(os.TempDir(), "ngm-test-.*")
	require.NoError(t, err)
	defer func() {
		err := os.RemoveAll(tmpDir)
		require.NoError(t, err)
	}()

	db, err := docdb.NewGenjiDBFromGenji(testutil.NewGenjiDB(t, tmpDir))
	require.NoError(t, err)
	defer db.Close()
	storage, err := NewProfileStorage(db)
	require.NoError(t, err)
	defer storage.Close()

	pt := meta.ProfileTarget{Kind: "profile", Component: "tidb", Address: "10.0.1.2"}
	t0 := time.Now()
	err = storage.AddProfile(pt, t0, nil, context.Canceled)
	require.NoError(t, err)
	t1 := t0.Add(time.Second)
	profile1Data := mockProfile()
	err = storage.AddProfile(pt, t1, profile1Data, nil)
	require.NoError(t, err)

	param := &meta.BasicQueryParam{Begin: t0.Unix(), End: t1.Unix()}
	profileLists, err := storage.QueryGroupProfiles(param)
	require.NoError(t, err)
	require.Equal(t, 1, len(profileLists))
	profileList := profileLists[0]
	require.Equal(t, 2, len(profileList.TsList))
	require.Equal(t, 2, len(profileList.ErrorList))
	require.Equal(t, t1.Unix(), profileList.TsList[0])
	require.Equal(t, "", profileList.ErrorList[0])
	require.Equal(t, t0.Unix(), profileList.TsList[1])
	require.Equal(t, context.Canceled.Error(), profileList.ErrorList[1])

	err = storage.QueryProfileData(param, func(pt meta.ProfileTarget, ts int64, data []byte) error {
		require.Equal(t, t1.Unix(), ts)
		require.True(t, bytes.Equal(profile1Data, data))
		return nil
	})
	require.NoError(t, err)
}

func TestGenjiDBAddColumn(t *testing.T) {
	tmpDir, err := os.MkdirTemp(os.TempDir(), "ngm-test-genjidb.*")
	require.NoError(t, err)
	defer func() {
		err := os.RemoveAll(tmpDir)
		require.NoError(t, err)
	}()
	db := testutil.NewGenjiDB(t, tmpDir)
	sql := "CREATE TABLE IF NOT EXISTS metaTable (ts INTEGER PRIMARY KEY)"
	err = db.Exec(sql)
	require.NoError(t, err)
	sql = "INSERT INTO metaTable (ts) VALUES (1)"
	err = db.Exec(sql)
	require.NoError(t, err)

	err = db.Close()
	require.NoError(t, err)

	// mock after upgrade, insert with a new column
	db = testutil.NewGenjiDB(t, tmpDir)
	sql = "INSERT INTO metaTable (ts, error) VALUES (2,'error')"
	err = db.Exec(sql)
	require.NoError(t, err)

	query := "SELECT ts, error FROM metaTable WHERE ts > 0 ORDER BY ts"
	res, err := db.Query(query)
	require.NoError(t, err)

	expectTsList := []int64{1, 2}
	expectErrorList := []string{"", "error"}
	idx := 0
	err = res.Iterate(func(d types.Document) error {
		var ts int64
		var errStr string
		err = document.Scan(d, &ts, &errStr)
		require.NoError(t, err)
		require.Equal(t, expectTsList[idx], ts)
		require.Equal(t, expectErrorList[idx], errStr)
		idx++
		return nil
	})
	require.Equal(t, 2, idx)
	require.NoError(t, err)
	err = res.Close()
	require.NoError(t, err)
	err = db.Close()
	require.NoError(t, err)
}

func TestGenjiDBGCBug(t *testing.T) {
	// related issue: https://github.com/genjidb/genji/issues/454
	tmpDir, err := os.MkdirTemp(os.TempDir(), "ngm-test-genjidb-gc.*")
	require.NoError(t, err)
	defer func() {
		err := os.RemoveAll(tmpDir)
		require.NoError(t, err)
	}()
	opts := badger.DefaultOptions(tmpDir).
		WithMemTableSize(1024 * 1024).
		WithNumLevelZeroTables(1).
		WithNumLevelZeroTablesStall(2).WithValueThreshold(128 * 1024)

	engine, err := badgerengine.NewEngine(opts)
	require.NoError(t, err)
	db, err := genji.New(context.Background(), engine)
	require.NoError(t, err)
	defer db.Close()
	badgerDB := engine.DB

	sql := "CREATE TABLE IF NOT EXISTS t (ts INTEGER PRIMARY KEY, data BLOB)"
	err = db.Exec(sql)
	require.NoError(t, err)
	// triage the bug in https://github.com/genjidb/genji/issues/454.
	err = db.Exec(sql)
	require.NoError(t, err)

	ts := time.Now().Unix()
	for i := 0; i < 1000; i++ {
		data := mockProfile()
		sql = "INSERT INTO t (ts, data) VALUES (?, ?)"
		err = db.Exec(sql, int(ts)+i, data)
		require.NoError(t, err)
	}

	sql = "DELETE FROM t WHERE ts <= ?"
	err = db.Exec(sql, int(ts)+1000)
	require.NoError(t, err)

	// Insert 1000 kv to make sure the deleted kv will been compacted.
	for i := 1000; i < 2000; i++ {
		data := mockProfile()
		sql = "INSERT INTO t (ts, data) VALUES (?, ?)"
		err = db.Exec(sql, int(ts)+i, data)
		require.NoError(t, err)
	}

	err = badgerDB.Flatten(runtime.NumCPU())
	require.NoError(t, err)

	keyCount := 0
	err = badgerDB.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.AllVersions = true
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			valueSize := it.Item().ValueSize()
			if valueSize <= 1024 {
				continue
			}
			keyCount++
		}
		return nil
	})
	require.NoError(t, err)

	// Without the pr-fix, the keyCount will be 2000, since those deleted keys don't been gc.
	require.Equal(t, keyCount, 1000)

	err = db.Close()
	require.NoError(t, err)
}

func mockProfile() []byte {
	size := 50*1024 + rand.Intn(50*1024)
	data := make([]byte, size)
	for i := 0; i < size; i += 99 {
		data[i] = byte(i % 255)
	}
	return data
}
