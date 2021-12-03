package store

import (
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/pingcap/ng_monitoring/component/conprof/meta"
	"github.com/pingcap/ng_monitoring/component/conprof/util"
	"github.com/pingcap/ng_monitoring/config"
	"github.com/pingcap/ng_monitoring/utils/testutil"
	"github.com/stretchr/testify/require"
)

func TestProfileStorage(t *testing.T) {
	tmpDir, err := ioutil.TempDir(os.TempDir(), "ngm-test-.*")
	require.NoError(t, err)
	defer func() {
		err := os.RemoveAll(tmpDir)
		require.NoError(t, err)
	}()
	baseTs := util.GetTimeStamp(time.Now())
	for i := 0; i < 2; i++ {
		testProfileStorage(t, tmpDir, baseTs+int64(i*1000), i > 0)
	}

	testProfileStorageGC(t, tmpDir, baseTs)
}

func testProfileStorageGC(t *testing.T, tmpDir string, baseTs int64) {
	genjiDB := testutil.NewGenjiDB(t, tmpDir)
	defer genjiDB.Close()
	storage, err := NewProfileStorage(genjiDB)
	require.NoError(t, err)
	defer storage.Close()

	originSize := len(storage.metaCache)
	require.True(t, originSize > 0)

	cfg := config.GetDefaultConfig()
	cfg.ContinueProfiling.DataRetentionSeconds = -100000
	config.StoreGlobalConfig(&cfg)
	storage.runGC()
	require.Equal(t, 0, len(storage.metaCache))
}

func testProfileStorage(t *testing.T, tmpDir string, baseTs int64, cleanCache bool) {
	genjiDB := testutil.NewGenjiDB(t, tmpDir)
	defer genjiDB.Close()
	storage, err := NewProfileStorage(genjiDB)
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
		err = storage.AddProfile(pt, ts, ca.data)
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

func mockProfile() []byte {
	size := 50*1024 + rand.Intn(50*1024)
	data := make([]byte, size)
	for i := 0; i < size; i += 99 {
		data[i] = byte(i % 255)
	}
	return data
}
