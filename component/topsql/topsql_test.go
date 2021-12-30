package topsql_test

import (
	"encoding/hex"
	"io/ioutil"
	"os"
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
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/suite"
)

func TestTopSQL(t *testing.T) {
	suite.Run(t, &testTopSQLSuite{})
}

var now = uint64(time.Now().Unix())
var testBaseTs = now - (now % 10000)

type testTopSQLSuite struct {
	suite.Suite

	dq *query.DefaultQuery
	ds *store.DefaultStore

	dir string
}

func (s *testTopSQLSuite) SetupSuite() {
	dir, err := ioutil.TempDir("", "topsql-test")
	s.NoError(err)

	s.dir = dir
	cfg := config.GetDefaultConfig()
	cfg.Storage.Path = dir

	database.Init(&cfg)

	ds, err := store.NewDefaultStore(timeseries.InsertHandler, document.Get())
	s.NoError(err)
	s.ds = ds

	dq := query.NewDefaultQuery(timeseries.SelectHandler, document.Get())
	s.dq = dq
}

func (s *testTopSQLSuite) TearDownSuite() {
	s.ds.Close()
	s.dq.Close()
	database.Stop()
	s.NoError(os.RemoveAll(s.dir))
}

func (s *testTopSQLSuite) TestInstances() {
	var err error
	err = s.ds.Instances([]store.InstanceItem{{
		Instance:     "127.0.0.1:10080",
		InstanceType: "tidb",
		TimestampSec: now - 30,
	}, {
		Instance:     "127.0.0.1:10080",
		InstanceType: "tidb",
		TimestampSec: now - 30, // same ts
	}, {
		Instance:     "127.0.0.1:10080",
		InstanceType: "tidb",
		TimestampSec: now,
	}, {
		Instance:     "127.0.0.1:10080",
		InstanceType: "tidb",
		TimestampSec: now - 20, // not in order
	}, {
		Instance:     "127.0.0.1:10081",
		InstanceType: "tidb",
		TimestampSec: now - 40,
	}, {
		Instance:     "127.0.0.1:20160",
		InstanceType: "tikv",
		TimestampSec: now,
	}})
	s.NoError(err)

	vmstorage.Storage.DebugFlush()
	var r []query.InstanceItem

	r = nil
	err = s.dq.Instances(int(now-41), int(now-40), &r)
	s.NoError(err)

	s.Equal(r, []query.InstanceItem{{
		Instance:     "127.0.0.1:10081",
		InstanceType: "tidb",
	}})

	r = nil
	err = s.dq.Instances(int(now-21), int(now-20), &r)
	s.NoError(err)

	s.Equal(r, []query.InstanceItem{{
		Instance:     "127.0.0.1:10080",
		InstanceType: "tidb",
	}})

	r = nil
	err = s.dq.Instances(int(now-1), int(now), &r)
	s.NoError(err)

	sort.Slice(r, func(i, j int) bool { return r[i].Instance < r[j].Instance })
	s.Equal(r, []query.InstanceItem{{
		Instance:     "127.0.0.1:10080",
		InstanceType: "tidb",
	}, {
		Instance:     "127.0.0.1:20160",
		InstanceType: "tikv",
	}})

	r = nil
	err = s.dq.Instances(int(now-40), int(now), &r)
	s.NoError(err)

	sort.Slice(r, func(i, j int) bool { return r[i].Instance < r[j].Instance })
	s.Equal(r, []query.InstanceItem{{
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
	err = s.dq.Instances(int(now-10), int(now), &r)
	s.NoError(err)

	sort.Slice(r, func(i, j int) bool { return r[i].Instance < r[j].Instance })
	s.Equal(r, []query.InstanceItem{{
		Instance:     "127.0.0.1:10080",
		InstanceType: "tidb",
	}, {
		Instance:     "127.0.0.1:20160",
		InstanceType: "tikv",
	}})

	r = nil
	err = s.dq.Instances(int(now-40), int(now-20), &r)
	s.NoError(err)

	sort.Slice(r, func(i, j int) bool { return r[i].Instance < r[j].Instance })
	s.Equal(r, []query.InstanceItem{{
		Instance:     "127.0.0.1:10080",
		InstanceType: "tidb",
	}, {
		Instance:     "127.0.0.1:10081",
		InstanceType: "tidb",
	}})
}

type testData struct {
	sqlDigest string
	plans     []testPlan
}

type testPlan struct {
	planDigest string
	ts         []uint64
	cpu        []uint64
	exec       []uint64
	duration   []uint64
}

func (s *testTopSQLSuite) TestSummary() {
	instance := "127.0.0.1:10080"
	instanceType := "tidb"

	// sql-0:
	//     plan-0: ts:       testBaseTs+0, testBaseTs+10, testBaseTs+20, testBaseTs+30, testBaseTs+40
	//             cpu:      67, 19, 54, 53, 71
	//             exec:     49, 11, 74, 72, 98
	//             duration: 97, 82, 24, 44, 88
	//  <unknown>: ts:       testBaseTs+0, testBaseTs+10, testBaseTs+20, testBaseTs+30, testBaseTs+40
	//             cpu:      85, 64, 43, 19, 31
	//             exec:     40, 92, 38, 87, 21
	//             duration: 11, 69, 58, 21, 56
	// sql-1:
	//     plan-0: ts:       testBaseTs+0, testBaseTs+10, testBaseTs+20, testBaseTs+30, testBaseTs+40
	//             cpu:      97, 46, 29, 22, 35
	//             exec:     68, 86, 24, 70, 75
	//             duration: 90, 59, 46, 80, 16
	//     plan-1: ts:       testBaseTs+0, testBaseTs+10, testBaseTs+20, testBaseTs+30, testBaseTs+40
	//             cpu:      51, 99, 14, 65, 27
	//             exec:     16, 11, 96, 73, 31
	//             duration: 22, 11, 77, 84, 33
	// sql-2:
	//     plan-0: ts:       testBaseTs+0, testBaseTs+10, testBaseTs+20, testBaseTs+30, testBaseTs+40
	//             cpu:      61, 87, 37, 55, 53
	//             exec:     54, 98, 46, 35, 52
	//             duration: 50, 46, 19, 63, 81
	data := []testData{{
		sqlDigest: "sql-0",
		plans: []testPlan{{
			planDigest: "plan-0",
			ts:         []uint64{testBaseTs + 0, testBaseTs + 10, testBaseTs + 20, testBaseTs + 30, testBaseTs + 40},
			cpu:        []uint64{67, 19, 54, 53, 71},
			exec:       []uint64{49, 11, 74, 72, 98},
			duration:   []uint64{97, 82, 24, 44, 88},
		}, {
			ts:       []uint64{testBaseTs + 0, testBaseTs + 10, testBaseTs + 20, testBaseTs + 30, testBaseTs + 40},
			cpu:      []uint64{85, 64, 43, 19, 31},
			exec:     []uint64{40, 92, 38, 87, 21},
			duration: []uint64{11, 69, 58, 21, 56},
		}},
	}, {
		sqlDigest: "sql-1",
		plans: []testPlan{{
			planDigest: "plan-0",
			ts:         []uint64{testBaseTs + 0, testBaseTs + 10, testBaseTs + 20, testBaseTs + 30, testBaseTs + 40},
			cpu:        []uint64{97, 46, 29, 22, 35},
			exec:       []uint64{68, 86, 24, 70, 75},
			duration:   []uint64{90, 59, 46, 80, 16},
		}, {
			planDigest: "plan-1",
			ts:         []uint64{testBaseTs + 0, testBaseTs + 10, testBaseTs + 20, testBaseTs + 30, testBaseTs + 40},
			cpu:        []uint64{51, 99, 14, 65, 27},
			exec:       []uint64{16, 11, 96, 73, 31},
			duration:   []uint64{22, 11, 77, 84, 33},
		}},
	}, {
		sqlDigest: "sql-2",
		plans: []testPlan{{
			planDigest: "plan-0",
			ts:         []uint64{testBaseTs + 0, testBaseTs + 10, testBaseTs + 20, testBaseTs + 30, testBaseTs + 40},
			cpu:        []uint64{61, 87, 37, 55, 53},
			exec:       []uint64{54, 98, 46, 35, 52},
			duration:   []uint64{50, 46, 19, 63, 81},
		}},
	}}

	for _, datum := range data {
		for _, plan := range datum.plans {
			for i := range plan.ts {
				s.NoError(s.ds.TopSQLRecord(instance, instanceType, &tipb.TopSQLRecord{
					SqlDigest:  []byte(datum.sqlDigest),
					PlanDigest: []byte(plan.planDigest),
					Items: []*tipb.TopSQLRecordItem{{
						TimestampSec:      plan.ts[i],
						CpuTimeMs:         uint32(plan.cpu[i]),
						StmtExecCount:     plan.exec[i],
						StmtDurationSumNs: plan.duration[i] * 1000000}}}))
			}
		}
	}

	vmstorage.Storage.DebugFlush()

	// normal case
	var res []query.SummaryItem
	err := s.dq.Summary(int(testBaseTs), int(testBaseTs+40), 10, 5, instance, instanceType, &res)
	s.NoError(err)
	s.sortSummary(res)
	s.Equal(res, []query.SummaryItem{{
		SQLDigest: hex.EncodeToString([]byte("sql-0")),
		Plans: []query.SummaryPlanItem{{
			TimestampSec:      []uint64{testBaseTs, testBaseTs + 10, testBaseTs + 20, testBaseTs + 30, testBaseTs + 40},
			CPUTimeMs:         []uint64{85, 64, 43, 19, 31},
			ExecCountPerSec:   278.0 / 41,
			DurationPerExecMs: 215.0 / 278.0,
		}, {
			PlanDigest:        hex.EncodeToString([]byte("plan-0")),
			TimestampSec:      []uint64{testBaseTs, testBaseTs + 10, testBaseTs + 20, testBaseTs + 30, testBaseTs + 40},
			CPUTimeMs:         []uint64{67, 19, 54, 53, 71},
			ExecCountPerSec:   304.0 / 41,
			DurationPerExecMs: 335.0 / 304.0,
		}},
	}, {
		SQLDigest: hex.EncodeToString([]byte("sql-1")),
		Plans: []query.SummaryPlanItem{{
			PlanDigest:        hex.EncodeToString([]byte("plan-0")),
			TimestampSec:      []uint64{testBaseTs + 0, testBaseTs + 10, testBaseTs + 20, testBaseTs + 30, testBaseTs + 40},
			CPUTimeMs:         []uint64{97, 46, 29, 22, 35},
			ExecCountPerSec:   323.0 / 41,
			DurationPerExecMs: 291.0 / 323.0,
		}, {
			PlanDigest:        hex.EncodeToString([]byte("plan-1")),
			TimestampSec:      []uint64{testBaseTs + 0, testBaseTs + 10, testBaseTs + 20, testBaseTs + 30, testBaseTs + 40},
			CPUTimeMs:         []uint64{51, 99, 14, 65, 27},
			ExecCountPerSec:   227.0 / 41,
			DurationPerExecMs: 227.0 / 227.0,
		}},
	}, {
		SQLDigest: hex.EncodeToString([]byte("sql-2")),
		Plans: []query.SummaryPlanItem{{
			PlanDigest:        hex.EncodeToString([]byte("plan-0")),
			TimestampSec:      []uint64{testBaseTs + 0, testBaseTs + 10, testBaseTs + 20, testBaseTs + 30, testBaseTs + 40},
			CPUTimeMs:         []uint64{61, 87, 37, 55, 53},
			ExecCountPerSec:   285.0 / 41,
			DurationPerExecMs: 259.0 / 285.0,
		}},
	}})

	// top 1
	res = nil
	err = s.dq.Summary(int(testBaseTs), int(testBaseTs+40), 10, 1, instance, instanceType, &res)
	s.NoError(err)
	s.sortSummary(res)
	s.Equal(res, []query.SummaryItem{{
		IsOther: true,
		Plans: []query.SummaryPlanItem{{
			TimestampSec:      []uint64{testBaseTs + 0, testBaseTs + 10, testBaseTs + 20, testBaseTs + 30, testBaseTs + 40},
			CPUTimeMs:         []uint64{209, 232, 80, 142, 115},
			ExecCountPerSec:   835.0 / 41,
			DurationPerExecMs: 777.0 / 835.0,
		}},
	}, {
		SQLDigest: hex.EncodeToString([]byte("sql-0")),
		Plans: []query.SummaryPlanItem{{
			TimestampSec:      []uint64{testBaseTs, testBaseTs + 10, testBaseTs + 20, testBaseTs + 30, testBaseTs + 40},
			CPUTimeMs:         []uint64{85, 64, 43, 19, 31},
			ExecCountPerSec:   278.0 / 41,
			DurationPerExecMs: 215.0 / 278.0,
		}, {
			PlanDigest:        hex.EncodeToString([]byte("plan-0")),
			TimestampSec:      []uint64{testBaseTs, testBaseTs + 10, testBaseTs + 20, testBaseTs + 30, testBaseTs + 40},
			CPUTimeMs:         []uint64{67, 19, 54, 53, 71},
			ExecCountPerSec:   304.0 / 41,
			DurationPerExecMs: 335.0 / 304.0,
		}},
	}})

	// no data
	res = nil
	err = s.dq.Summary(int(testBaseTs+41), int(testBaseTs+100), 10, 5, instance, instanceType, &res)
	s.NoError(err)
	s.Equal(len(res), 0)

	// one point
	res = nil
	err = s.dq.Summary(int(testBaseTs+40), int(testBaseTs+40), 10, 5, instance, instanceType, &res)
	s.NoError(err)
	s.sortSummary(res)
	s.Equal(res, []query.SummaryItem{{
		SQLDigest: hex.EncodeToString([]byte("sql-0")),
		Plans: []query.SummaryPlanItem{{
			TimestampSec:      []uint64{testBaseTs + 40},
			CPUTimeMs:         []uint64{31},
			ExecCountPerSec:   21.0,
			DurationPerExecMs: 56.0 / 21.0,
		}, {
			PlanDigest:        hex.EncodeToString([]byte("plan-0")),
			TimestampSec:      []uint64{testBaseTs + 40},
			CPUTimeMs:         []uint64{71},
			ExecCountPerSec:   98.0,
			DurationPerExecMs: 88.0 / 98.0,
		}},
	}, {
		SQLDigest: hex.EncodeToString([]byte("sql-1")),
		Plans: []query.SummaryPlanItem{{
			PlanDigest:        hex.EncodeToString([]byte("plan-0")),
			TimestampSec:      []uint64{testBaseTs + 40},
			CPUTimeMs:         []uint64{35},
			ExecCountPerSec:   75.0,
			DurationPerExecMs: 16.0 / 75.0,
		}, {
			PlanDigest:        hex.EncodeToString([]byte("plan-1")),
			TimestampSec:      []uint64{testBaseTs + 40},
			CPUTimeMs:         []uint64{27},
			ExecCountPerSec:   31.0,
			DurationPerExecMs: 33.0 / 31.0,
		}},
	}, {
		SQLDigest: hex.EncodeToString([]byte("sql-2")),
		Plans: []query.SummaryPlanItem{{
			PlanDigest:        hex.EncodeToString([]byte("plan-0")),
			TimestampSec:      []uint64{testBaseTs + 40},
			CPUTimeMs:         []uint64{53},
			ExecCountPerSec:   52.0,
			DurationPerExecMs: 81.0 / 52.0,
		}},
	}})

	// two points
	res = nil
	err = s.dq.Summary(int(testBaseTs+19), int(testBaseTs+32), 10, 5, instance, instanceType, &res)
	s.NoError(err)
	s.sortSummary(res)
	s.Equal(res, []query.SummaryItem{{
		SQLDigest: hex.EncodeToString([]byte("sql-0")),
		Plans: []query.SummaryPlanItem{{
			TimestampSec:      []uint64{testBaseTs + 22, testBaseTs + 32},
			CPUTimeMs:         []uint64{43, 19},
			ExecCountPerSec:   125.0 / 14,
			DurationPerExecMs: 79.0 / 125.0,
		}, {
			PlanDigest:        hex.EncodeToString([]byte("plan-0")),
			TimestampSec:      []uint64{testBaseTs + 22, testBaseTs + 32},
			CPUTimeMs:         []uint64{54, 53},
			ExecCountPerSec:   146.0 / 14,
			DurationPerExecMs: 68.0 / 146.0,
		}},
	}, {
		SQLDigest: hex.EncodeToString([]byte("sql-1")),
		Plans: []query.SummaryPlanItem{{
			PlanDigest:        hex.EncodeToString([]byte("plan-0")),
			TimestampSec:      []uint64{testBaseTs + 22, testBaseTs + 32},
			CPUTimeMs:         []uint64{29, 22},
			ExecCountPerSec:   94.0 / 14,
			DurationPerExecMs: 126.0 / 94.0,
		}, {
			PlanDigest:        hex.EncodeToString([]byte("plan-1")),
			TimestampSec:      []uint64{testBaseTs + 22, testBaseTs + 32},
			CPUTimeMs:         []uint64{14, 65},
			ExecCountPerSec:   169.0 / 14,
			DurationPerExecMs: 161.0 / 169.0,
		}},
	}, {
		SQLDigest: hex.EncodeToString([]byte("sql-2")),
		Plans: []query.SummaryPlanItem{{
			PlanDigest:        hex.EncodeToString([]byte("plan-0")),
			TimestampSec:      []uint64{testBaseTs + 22, testBaseTs + 32},
			CPUTimeMs:         []uint64{37, 55},
			ExecCountPerSec:   81.0 / 14,
			DurationPerExecMs: 82.0 / 81.0,
		}},
	}})
}

func (s *testTopSQLSuite) sortSummary(res []query.SummaryItem) {
	sort.Slice(res, func(i, j int) bool {
		sqli, err := hex.DecodeString(res[i].SQLDigest)
		s.NoError(err)
		sqlj, err := hex.DecodeString(res[j].SQLDigest)
		s.NoError(err)
		return string(sqli) < string(sqlj)
	})

	for _, r := range res {
		sort.Slice(r.Plans, func(i, j int) bool {
			plani, err := hex.DecodeString(r.Plans[i].PlanDigest)
			s.NoError(err)
			planj, err := hex.DecodeString(r.Plans[j].PlanDigest)
			s.NoError(err)
			return string(plani) < string(planj)
		})
	}
}
