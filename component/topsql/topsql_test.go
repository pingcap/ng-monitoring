package topsql_test

import (
	"sort"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmstorage"
	"github.com/pingcap/ng-monitoring/component/topsql/query"
	"github.com/pingcap/ng-monitoring/component/topsql/store"
	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/ng-monitoring/database"
	"github.com/pingcap/ng-monitoring/database/document"
	"github.com/pingcap/ng-monitoring/database/timeseries"
	"github.com/stretchr/testify/require"
)

func TestInstancesBasic(t *testing.T) {
	cfg := config.GetDefaultConfig()
	cfg.Storage.Path = t.TempDir()

	database.Init(&cfg)
	defer database.Stop()

	ds, err := store.NewDefaultStore(timeseries.InsertHandler, document.Get())
	require.NoError(t, err)
	defer ds.Close()

	dq := query.NewDefaultQuery(timeseries.SelectHandler, document.Get())
	defer dq.Close()

	now := time.Now().Unix()
	err = ds.Instances([]store.InstanceItem{{
		Instance:     "127.0.0.1:10080",
		InstanceType: "tidb",
		TimestampSec: uint64(now - 30),
	}, {
		Instance:     "127.0.0.1:10080",
		InstanceType: "tidb",
		TimestampSec: uint64(now - 30), // same ts
	}, {
		Instance:     "127.0.0.1:10080",
		InstanceType: "tidb",
		TimestampSec: uint64(now),
	}, {
		Instance:     "127.0.0.1:10080",
		InstanceType: "tidb",
		TimestampSec: uint64(now - 20), // not in order
	}, {
		Instance:     "127.0.0.1:10081",
		InstanceType: "tidb",
		TimestampSec: uint64(now - 40),
	}, {
		Instance:     "127.0.0.1:20160",
		InstanceType: "tikv",
		TimestampSec: uint64(now),
	}})
	require.NoError(t, err)

	vmstorage.Storage.DebugFlush()
	var r []query.InstanceItem

	r = nil
	err = dq.Instances(int(now-41), int(now-40), &r)
	require.NoError(t, err)
	require.Equal(t, r, []query.InstanceItem{{
		Instance:     "127.0.0.1:10081",
		InstanceType: "tidb",
	}})

	r = nil
	err = dq.Instances(int(now-21), int(now-20), &r)
	require.NoError(t, err)
	require.Equal(t, r, []query.InstanceItem{{
		Instance:     "127.0.0.1:10080",
		InstanceType: "tidb",
	}})

	r = nil
	err = dq.Instances(int(now-1), int(now), &r)
	require.NoError(t, err)
	sort.Slice(r, func(i, j int) bool { return r[i].Instance < r[j].Instance })
	require.Equal(t, r, []query.InstanceItem{{
		Instance:     "127.0.0.1:10080",
		InstanceType: "tidb",
	}, {
		Instance:     "127.0.0.1:20160",
		InstanceType: "tikv",
	}})

	r = nil
	err = dq.Instances(int(now-40), int(now), &r)
	require.NoError(t, err)
	sort.Slice(r, func(i, j int) bool { return r[i].Instance < r[j].Instance })
	require.Equal(t, r, []query.InstanceItem{{
		Instance:     "127.0.0.1:10080",
		InstanceType: "tidb",
	}, {
		Instance:     "127.0.0.1:10081",
		InstanceType: "tidb",
	}, {
		Instance:     "127.0.0.1:20160",
		InstanceType: "tikv",
	}})

	r = nil
	err = dq.Instances(int(now-10), int(now), &r)
	require.NoError(t, err)
	sort.Slice(r, func(i, j int) bool { return r[i].Instance < r[j].Instance })
	require.Equal(t, r, []query.InstanceItem{{
		Instance:     "127.0.0.1:10080",
		InstanceType: "tidb",
	}, {
		Instance:     "127.0.0.1:20160",
		InstanceType: "tikv",
	}})

	r = nil
	err = dq.Instances(int(now-40), int(now-20), &r)
	require.NoError(t, err)

	sort.Slice(r, func(i, j int) bool { return r[i].Instance < r[j].Instance })
	require.Equal(t, r, []query.InstanceItem{{
		Instance:     "127.0.0.1:10080",
		InstanceType: "tidb",
	}, {
		Instance:     "127.0.0.1:10081",
		InstanceType: "tidb",
	}})
}
