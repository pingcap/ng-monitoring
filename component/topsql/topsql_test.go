package topsql_test

import (
	"encoding/hex"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/ng-monitoring/component/topsql/query"
	"github.com/pingcap/ng-monitoring/component/topsql/store"
	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/ng-monitoring/database"
	"github.com/pingcap/ng-monitoring/database/docdb"
	"github.com/pingcap/ng-monitoring/database/timeseries"
	"github.com/pingcap/ng-monitoring/utils/testutil"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmstorage"
	rsmetering "github.com/pingcap/kvproto/pkg/resource_usage_agent"
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

	dq    *query.DefaultQuery
	ds    *store.DefaultStore
	docDB docdb.DocDB

	dir string
}

func (s *testTopSQLSuite) SetupSuite() {
	dir, err := os.MkdirTemp("", "topsql-test")
	s.NoError(err)

	s.dir = dir
	cfg := config.GetDefaultConfig()
	cfg.Storage.Path = dir

	database.Init(&cfg)
	s.docDB, err = docdb.NewGenjiDBFromGenji(testutil.NewGenjiDB(s.T(), dir))
	s.NoError(err)
	s.ds = store.NewDefaultStore(timeseries.InsertHandler, s.docDB, 0)
	s.dq = query.NewDefaultQuery(timeseries.SelectHandler, s.docDB)
}

func (s *testTopSQLSuite) TearDownSuite() {
	s.ds.Close()
	s.dq.Close()
	database.Stop()
	s.docDB.Close()
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
	err = s.dq.Instances(int(now-40), int(now-40), &r)
	s.NoError(err)
	s.Equal(r, []query.InstanceItem{{
		Instance:     "127.0.0.1:10081",
		InstanceType: "tidb",
	}})

	r = nil
	err = s.dq.Instances(int(now-20), int(now-20), &r)
	s.NoError(err)
	s.Equal(r, []query.InstanceItem{{
		Instance:     "127.0.0.1:10080",
		InstanceType: "tidb",
	}})

	r = nil
	err = s.dq.Instances(int(now), int(now), &r)
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

	r = nil
	err = s.dq.Instances(int(now-31), int(now-21), &r)
	s.NoError(err)
	s.Equal(r, []query.InstanceItem{{
		Instance:     "127.0.0.1:10080",
		InstanceType: "tidb",
	}})
}

func (s *testTopSQLSuite) TestTiDBSummary() {
	type tidbPlanItem struct {
		ts            []uint64
		cpu           []uint64
		exec          []uint64
		duration      []uint64
		durationCount []uint64
	}
	type tidbTestPlan map[string]tidbPlanItem
	type tidbTestData map[string]tidbTestPlan

	instance := "127.0.0.1:10080"
	instanceType := "tidb"

	// sql-0:
	//  <unknown>: ts:       testBaseTs+0, testBaseTs+10, testBaseTs+20, testBaseTs+30, testBaseTs+40
	//             cpu:      85, 64, 43, 19, 31
	//             exec:     40, 92, 38, 87, 21
	//             duration: 11, 69, 58, 21, 56
	//     plan-0: ts:       testBaseTs+0, testBaseTs+10, testBaseTs+20, testBaseTs+30, testBaseTs+40
	//             cpu:      67, 19, 54, 53, 71
	//             exec:     49, 11, 74, 72, 98
	//             duration: 97, 82, 24, 44, 88
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
	data := tidbTestData{
		"sql-0": {
			"": {
				ts:            []uint64{testBaseTs + 0, testBaseTs + 10, testBaseTs + 20, testBaseTs + 30, testBaseTs + 40},
				cpu:           []uint64{85, 64, 43, 19, 31},
				exec:          []uint64{40, 92, 38, 87, 21},
				duration:      []uint64{11, 69, 58, 21, 56},
				durationCount: []uint64{40, 92, 38, 87, 21},
			},
			"plan-0": {
				ts:            []uint64{testBaseTs + 0, testBaseTs + 10, testBaseTs + 20, testBaseTs + 30, testBaseTs + 40},
				cpu:           []uint64{67, 19, 54, 53, 71},
				exec:          []uint64{49, 11, 74, 72, 98},
				duration:      []uint64{97, 82, 24, 44, 88},
				durationCount: []uint64{49, 11, 74, 72, 98},
			},
		},
		"sql-1": {
			"plan-0": {
				ts:            []uint64{testBaseTs + 0, testBaseTs + 10, testBaseTs + 20, testBaseTs + 30, testBaseTs + 40},
				cpu:           []uint64{97, 46, 29, 22, 35},
				exec:          []uint64{68, 86, 24, 70, 75},
				duration:      []uint64{90, 59, 46, 80, 16},
				durationCount: []uint64{68, 86, 24, 70, 75},
			},
			"plan-1": {
				ts:            []uint64{testBaseTs + 0, testBaseTs + 10, testBaseTs + 20, testBaseTs + 30, testBaseTs + 40},
				cpu:           []uint64{51, 99, 14, 65, 27},
				exec:          []uint64{16, 11, 96, 73, 31},
				duration:      []uint64{22, 11, 77, 84, 33},
				durationCount: []uint64{16, 11, 96, 73, 31},
			},
		},
		"sql-2": {
			"plan-0": {
				ts:            []uint64{testBaseTs + 0, testBaseTs + 10, testBaseTs + 20, testBaseTs + 30, testBaseTs + 40},
				cpu:           []uint64{61, 87, 37, 55, 53},
				exec:          []uint64{54, 98, 46, 35, 52},
				duration:      []uint64{50, 46, 19, 63, 81},
				durationCount: []uint64{54, 98, 46, 35, 52},
			},
		},
	}

	for sqlDigest, plans := range data {
		for planDigest, planItem := range plans {
			var items []*tipb.TopSQLRecordItem
			for j := range planItem.ts {
				items = append(items, &tipb.TopSQLRecordItem{
					TimestampSec:      planItem.ts[j],
					CpuTimeMs:         uint32(planItem.cpu[j]),
					StmtExecCount:     planItem.exec[j],
					StmtDurationSumNs: planItem.duration[j] * 1_000_000,
					StmtDurationCount: planItem.durationCount[j],
				})
			}

			s.NoError(s.ds.TopSQLRecord(instance, instanceType, &tipb.TopSQLRecord{
				SqlDigest:  []byte(sqlDigest),
				PlanDigest: []byte(planDigest),
				Items:      items,
			}))
		}
	}

	vmstorage.Storage.DebugFlush()

	// normal case
	var res []query.SummaryItem
	err := s.dq.Summary(int(testBaseTs), int(testBaseTs+40), 10, 5, instance, instanceType, &res)
	s.NoError(err)
	s.sortSummary(res)
	tsList := []uint64{testBaseTs + 0, testBaseTs + 10, testBaseTs + 20, testBaseTs + 30, testBaseTs + 40}
	timeWindow := float64(40 + 1)
	s.Equal(res, []query.SummaryItem{{
		SQLDigest:         hex.EncodeToString([]byte("sql-0")),
		CPUTimeMs:         sum(data["sql-0"][""].cpu, data["sql-0"]["plan-0"].cpu),
		ExecCountPerSec:   sumf(data["sql-0"][""].exec, data["sql-0"]["plan-0"].exec) / timeWindow,
		DurationPerExecMs: sumf(data["sql-0"][""].duration, data["sql-0"]["plan-0"].duration) / sumf(data["sql-0"][""].exec, data["sql-0"]["plan-0"].exec),
		Plans: []query.SummaryPlanItem{{
			TimestampSec:      tsList,
			CPUTimeMs:         data["sql-0"][""].cpu,
			ExecCountPerSec:   sumf(data["sql-0"][""].exec) / timeWindow,
			DurationPerExecMs: sumf(data["sql-0"][""].duration) / sumf(data["sql-0"][""].exec),
		}, {
			PlanDigest:        hex.EncodeToString([]byte("plan-0")),
			TimestampSec:      tsList,
			CPUTimeMs:         data["sql-0"]["plan-0"].cpu,
			ExecCountPerSec:   sumf(data["sql-0"]["plan-0"].exec) / timeWindow,
			DurationPerExecMs: sumf(data["sql-0"]["plan-0"].duration) / sumf(data["sql-0"]["plan-0"].exec),
		}},
	}, {
		SQLDigest:         hex.EncodeToString([]byte("sql-1")),
		CPUTimeMs:         sum(data["sql-1"]["plan-0"].cpu, data["sql-1"]["plan-1"].cpu),
		ExecCountPerSec:   sumf(data["sql-1"]["plan-0"].exec, data["sql-1"]["plan-1"].exec) / timeWindow,
		DurationPerExecMs: sumf(data["sql-1"]["plan-0"].duration, data["sql-1"]["plan-1"].duration) / sumf(data["sql-1"]["plan-0"].exec, data["sql-1"]["plan-1"].exec),
		Plans: []query.SummaryPlanItem{{
			PlanDigest:        hex.EncodeToString([]byte("plan-0")),
			TimestampSec:      tsList,
			CPUTimeMs:         data["sql-1"]["plan-0"].cpu,
			ExecCountPerSec:   sumf(data["sql-1"]["plan-0"].exec) / timeWindow,
			DurationPerExecMs: sumf(data["sql-1"]["plan-0"].duration) / sumf(data["sql-1"]["plan-0"].exec),
		}, {
			PlanDigest:        hex.EncodeToString([]byte("plan-1")),
			TimestampSec:      tsList,
			CPUTimeMs:         data["sql-1"]["plan-1"].cpu,
			ExecCountPerSec:   sumf(data["sql-1"]["plan-1"].exec) / timeWindow,
			DurationPerExecMs: sumf(data["sql-1"]["plan-1"].duration) / sumf(data["sql-1"]["plan-1"].exec),
		}},
	}, {
		SQLDigest:         hex.EncodeToString([]byte("sql-2")),
		CPUTimeMs:         sum(data["sql-2"]["plan-0"].cpu),
		ExecCountPerSec:   sumf(data["sql-2"]["plan-0"].exec) / timeWindow,
		DurationPerExecMs: sumf(data["sql-2"]["plan-0"].duration) / sumf(data["sql-2"]["plan-0"].exec),
		Plans: []query.SummaryPlanItem{{
			PlanDigest:        hex.EncodeToString([]byte("plan-0")),
			TimestampSec:      tsList,
			CPUTimeMs:         data["sql-2"]["plan-0"].cpu,
			ExecCountPerSec:   sumf(data["sql-2"]["plan-0"].exec) / timeWindow,
			DurationPerExecMs: sumf(data["sql-2"]["plan-0"].duration) / sumf(data["sql-2"]["plan-0"].exec),
		}},
	}})

	// top 1
	res = nil
	err = s.dq.Summary(int(testBaseTs), int(testBaseTs+40), 10, 1, instance, instanceType, &res)
	s.NoError(err)
	s.sortSummary(res)
	tsList = []uint64{testBaseTs + 0, testBaseTs + 10, testBaseTs + 20, testBaseTs + 30, testBaseTs + 40}
	timeWindow = 41
	s.Equal(res, []query.SummaryItem{{
		IsOther:           true,
		CPUTimeMs:         sum(data["sql-1"]["plan-0"].cpu, data["sql-1"]["plan-1"].cpu, data["sql-2"]["plan-0"].cpu),
		ExecCountPerSec:   sumf(data["sql-1"]["plan-0"].exec, data["sql-1"]["plan-1"].exec, data["sql-2"]["plan-0"].exec) / timeWindow,
		DurationPerExecMs: sumf(data["sql-1"]["plan-0"].duration, data["sql-1"]["plan-1"].duration, data["sql-2"]["plan-0"].duration) / sumf(data["sql-1"]["plan-0"].exec, data["sql-1"]["plan-1"].exec, data["sql-2"]["plan-0"].exec),
		Plans: []query.SummaryPlanItem{{
			TimestampSec:      tsList,
			CPUTimeMs:         sumv(data["sql-1"]["plan-0"].cpu, data["sql-1"]["plan-1"].cpu, data["sql-2"]["plan-0"].cpu),
			ExecCountPerSec:   sumf(data["sql-1"]["plan-0"].exec, data["sql-1"]["plan-1"].exec, data["sql-2"]["plan-0"].exec) / timeWindow,
			DurationPerExecMs: sumf(data["sql-1"]["plan-0"].duration, data["sql-1"]["plan-1"].duration, data["sql-2"]["plan-0"].duration) / sumf(data["sql-1"]["plan-0"].exec, data["sql-1"]["plan-1"].exec, data["sql-2"]["plan-0"].exec),
		}},
	}, {
		SQLDigest:         hex.EncodeToString([]byte("sql-0")),
		CPUTimeMs:         sum(data["sql-0"][""].cpu, data["sql-0"]["plan-0"].cpu),
		ExecCountPerSec:   sumf(data["sql-0"][""].exec, data["sql-0"]["plan-0"].exec) / timeWindow,
		DurationPerExecMs: sumf(data["sql-0"][""].duration, data["sql-0"]["plan-0"].duration) / sumf(data["sql-0"][""].exec, data["sql-0"]["plan-0"].exec),
		Plans: []query.SummaryPlanItem{{
			TimestampSec:      tsList,
			CPUTimeMs:         data["sql-0"][""].cpu,
			ExecCountPerSec:   sumf(data["sql-0"][""].exec) / timeWindow,
			DurationPerExecMs: sumf(data["sql-0"][""].duration) / sumf(data["sql-0"][""].exec),
		}, {
			PlanDigest:        hex.EncodeToString([]byte("plan-0")),
			TimestampSec:      tsList,
			CPUTimeMs:         data["sql-0"]["plan-0"].cpu,
			ExecCountPerSec:   sumf(data["sql-0"]["plan-0"].exec) / timeWindow,
			DurationPerExecMs: sumf(data["sql-0"]["plan-0"].duration) / sumf(data["sql-0"]["plan-0"].exec),
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
	tsList = []uint64{testBaseTs + 40}
	timeWindow = 1
	s.Equal(res, []query.SummaryItem{{
		SQLDigest:         hex.EncodeToString([]byte("sql-0")),
		CPUTimeMs:         sum(data["sql-0"][""].cpu[4:], data["sql-0"]["plan-0"].cpu[4:]),
		ExecCountPerSec:   sumf(data["sql-0"][""].exec[4:], data["sql-0"]["plan-0"].exec[4:]) / timeWindow,
		DurationPerExecMs: sumf(data["sql-0"][""].duration[4:], data["sql-0"]["plan-0"].duration[4:]) / sumf(data["sql-0"][""].exec[4:], data["sql-0"]["plan-0"].exec[4:]),
		Plans: []query.SummaryPlanItem{{
			TimestampSec:      tsList,
			CPUTimeMs:         data["sql-0"][""].cpu[4:],
			ExecCountPerSec:   sumf(data["sql-0"][""].exec[4:]) / timeWindow,
			DurationPerExecMs: sumf(data["sql-0"][""].duration[4:]) / sumf(data["sql-0"][""].exec[4:]),
		}, {
			PlanDigest:        hex.EncodeToString([]byte("plan-0")),
			TimestampSec:      tsList,
			CPUTimeMs:         data["sql-0"]["plan-0"].cpu[4:],
			ExecCountPerSec:   sumf(data["sql-0"]["plan-0"].exec[4:]) / timeWindow,
			DurationPerExecMs: sumf(data["sql-0"]["plan-0"].duration[4:]) / sumf(data["sql-0"]["plan-0"].exec[4:]),
		}},
	}, {
		SQLDigest:         hex.EncodeToString([]byte("sql-1")),
		CPUTimeMs:         sum(data["sql-1"]["plan-0"].cpu[4:], data["sql-1"]["plan-1"].cpu[4:]),
		ExecCountPerSec:   sumf(data["sql-1"]["plan-0"].exec[4:], data["sql-1"]["plan-1"].exec[4:]) / timeWindow,
		DurationPerExecMs: sumf(data["sql-1"]["plan-0"].duration[4:], data["sql-1"]["plan-1"].duration[4:]) / sumf(data["sql-1"]["plan-0"].exec[4:], data["sql-1"]["plan-1"].exec[4:]),
		Plans: []query.SummaryPlanItem{{
			PlanDigest:        hex.EncodeToString([]byte("plan-0")),
			TimestampSec:      tsList,
			CPUTimeMs:         data["sql-1"]["plan-0"].cpu[4:],
			ExecCountPerSec:   sumf(data["sql-1"]["plan-0"].exec[4:]) / timeWindow,
			DurationPerExecMs: sumf(data["sql-1"]["plan-0"].duration[4:]) / sumf(data["sql-1"]["plan-0"].exec[4:]),
		}, {
			PlanDigest:        hex.EncodeToString([]byte("plan-1")),
			TimestampSec:      tsList,
			CPUTimeMs:         data["sql-1"]["plan-1"].cpu[4:],
			ExecCountPerSec:   sumf(data["sql-1"]["plan-1"].exec[4:]) / timeWindow,
			DurationPerExecMs: sumf(data["sql-1"]["plan-1"].duration[4:]) / sumf(data["sql-1"]["plan-1"].exec[4:]),
		}},
	}, {
		SQLDigest:         hex.EncodeToString([]byte("sql-2")),
		CPUTimeMs:         sum(data["sql-2"]["plan-0"].cpu[4:]),
		ExecCountPerSec:   sumf(data["sql-2"]["plan-0"].exec[4:]) / timeWindow,
		DurationPerExecMs: sumf(data["sql-2"]["plan-0"].duration[4:]) / sumf(data["sql-2"]["plan-0"].exec[4:]),
		Plans: []query.SummaryPlanItem{{
			PlanDigest:        hex.EncodeToString([]byte("plan-0")),
			TimestampSec:      tsList,
			CPUTimeMs:         data["sql-2"]["plan-0"].cpu[4:],
			ExecCountPerSec:   sumf(data["sql-2"]["plan-0"].exec[4:]) / timeWindow,
			DurationPerExecMs: sumf(data["sql-2"]["plan-0"].duration[4:]) / sumf(data["sql-2"]["plan-0"].exec[4:]),
		}},
	}})

	// two points
	res = nil
	err = s.dq.Summary(int(testBaseTs+19), int(testBaseTs+32), 10, 5, instance, instanceType, &res)
	s.NoError(err)
	s.sortSummary(res)
	tsList = []uint64{testBaseTs + 22, testBaseTs + 32}
	timeWindow = 32 - 19 + 1
	s.Equal(res, []query.SummaryItem{{
		SQLDigest:         hex.EncodeToString([]byte("sql-0")),
		CPUTimeMs:         sum(data["sql-0"][""].cpu[2:4], data["sql-0"]["plan-0"].cpu[2:4]),
		ExecCountPerSec:   sumf(data["sql-0"][""].exec[2:4], data["sql-0"]["plan-0"].exec[2:4]) / timeWindow,
		DurationPerExecMs: sumf(data["sql-0"][""].duration[2:4], data["sql-0"]["plan-0"].duration[2:4]) / sumf(data["sql-0"][""].exec[2:4], data["sql-0"]["plan-0"].exec[2:4]),
		Plans: []query.SummaryPlanItem{{
			TimestampSec:      tsList,
			CPUTimeMs:         data["sql-0"][""].cpu[2:4],
			ExecCountPerSec:   sumf(data["sql-0"][""].exec[2:4]) / timeWindow,
			DurationPerExecMs: sumf(data["sql-0"][""].duration[2:4]) / sumf(data["sql-0"][""].exec[2:4]),
		}, {
			PlanDigest:        hex.EncodeToString([]byte("plan-0")),
			TimestampSec:      tsList,
			CPUTimeMs:         data["sql-0"]["plan-0"].cpu[2:4],
			ExecCountPerSec:   sumf(data["sql-0"]["plan-0"].exec[2:4]) / timeWindow,
			DurationPerExecMs: sumf(data["sql-0"]["plan-0"].duration[2:4]) / sumf(data["sql-0"]["plan-0"].exec[2:4]),
		}},
	}, {
		SQLDigest:         hex.EncodeToString([]byte("sql-1")),
		CPUTimeMs:         sum(data["sql-1"]["plan-0"].cpu[2:4], data["sql-1"]["plan-1"].cpu[2:4]),
		ExecCountPerSec:   sumf(data["sql-1"]["plan-0"].exec[2:4], data["sql-1"]["plan-1"].exec[2:4]) / timeWindow,
		DurationPerExecMs: sumf(data["sql-1"]["plan-0"].duration[2:4], data["sql-1"]["plan-1"].duration[2:4]) / sumf(data["sql-1"]["plan-0"].exec[2:4], data["sql-1"]["plan-1"].exec[2:4]),
		Plans: []query.SummaryPlanItem{{
			PlanDigest:        hex.EncodeToString([]byte("plan-0")),
			TimestampSec:      tsList,
			CPUTimeMs:         data["sql-1"]["plan-0"].cpu[2:4],
			ExecCountPerSec:   sumf(data["sql-1"]["plan-0"].exec[2:4]) / timeWindow,
			DurationPerExecMs: sumf(data["sql-1"]["plan-0"].duration[2:4]) / sumf(data["sql-1"]["plan-0"].exec[2:4]),
		}, {
			PlanDigest:        hex.EncodeToString([]byte("plan-1")),
			TimestampSec:      tsList,
			CPUTimeMs:         data["sql-1"]["plan-1"].cpu[2:4],
			ExecCountPerSec:   sumf(data["sql-1"]["plan-1"].exec[2:4]) / timeWindow,
			DurationPerExecMs: sumf(data["sql-1"]["plan-1"].duration[2:4]) / sumf(data["sql-1"]["plan-1"].exec[2:4]),
		}},
	}, {
		SQLDigest:         hex.EncodeToString([]byte("sql-2")),
		CPUTimeMs:         sum(data["sql-2"]["plan-0"].cpu[2:4]),
		ExecCountPerSec:   sumf(data["sql-2"]["plan-0"].exec[2:4]) / timeWindow,
		DurationPerExecMs: sumf(data["sql-2"]["plan-0"].duration[2:4]) / sumf(data["sql-2"]["plan-0"].exec[2:4]),
		Plans: []query.SummaryPlanItem{{
			PlanDigest:        hex.EncodeToString([]byte("plan-0")),
			TimestampSec:      tsList,
			CPUTimeMs:         data["sql-2"]["plan-0"].cpu[2:4],
			ExecCountPerSec:   sumf(data["sql-2"]["plan-0"].exec[2:4]) / timeWindow,
			DurationPerExecMs: sumf(data["sql-2"]["plan-0"].duration[2:4]) / sumf(data["sql-2"]["plan-0"].exec[2:4]),
		}},
	}})
}

func (s *testTopSQLSuite) TestTiKVSummaryBy() {
	type tikvItem struct {
		ts    []uint64
		cpu   []uint64
		reads []uint64
	}
	type tikvTestPlan map[string]tikvItem
	type tikvTestData map[string]map[string]tikvTestPlan // sql -> table -> data

	instance := "127.0.0.3:20180"
	tikvInstanceType := "tikv"

	data := tikvTestData{
		"sql-0": {
			"table-0": {
				"index": {
					ts:    []uint64{testBaseTs + 0, testBaseTs + 10, testBaseTs + 20, testBaseTs + 30, testBaseTs + 40},
					cpu:   []uint64{85, 64, 43, 19, 31},
					reads: []uint64{11, 69, 58, 21, 56},
				},
			},
			"table-1": {
				"record": {
					ts:    []uint64{testBaseTs + 0, testBaseTs + 10, testBaseTs + 20, testBaseTs + 30, testBaseTs + 40},
					cpu:   []uint64{67, 19, 54, 53, 71},
					reads: []uint64{97, 82, 24, 44, 88},
				},
				"index": {
					ts:    []uint64{testBaseTs + 0, testBaseTs + 10, testBaseTs + 20, testBaseTs + 30, testBaseTs + 40},
					cpu:   []uint64{49, 93, 12, 43, 95},
					reads: []uint64{80, 44, 12, 10, 40},
				},
			},
		},
		"sql-1": {
			"table-0": {
				"index": {
					ts:    []uint64{testBaseTs + 0, testBaseTs + 10, testBaseTs + 20, testBaseTs + 30, testBaseTs + 40},
					cpu:   []uint64{97, 46, 29, 22, 35},
					reads: []uint64{90, 59, 46, 80, 16},
				},
			},
			"table-1": {
				"record": {
					ts:    []uint64{testBaseTs + 0, testBaseTs + 10, testBaseTs + 20, testBaseTs + 30, testBaseTs + 40},
					cpu:   []uint64{51, 99, 14, 65, 27},
					reads: []uint64{22, 11, 77, 84, 33},
				},
			},
			"table-2": {
				"record": {
					ts:    []uint64{testBaseTs + 0, testBaseTs + 10, testBaseTs + 20, testBaseTs + 30, testBaseTs + 40},
					cpu:   []uint64{61, 64, 83, 99, 43},
					reads: []uint64{51, 93, 42, 27, 21},
				},
			},
		},
		"sql-2": {
			"table-2": {
				"record": {
					ts:    []uint64{testBaseTs + 0, testBaseTs + 10, testBaseTs + 20, testBaseTs + 30, testBaseTs + 40},
					cpu:   []uint64{61, 87, 37, 55, 53},
					reads: []uint64{50, 46, 19, 63, 81},
				},
			},
		},
	}
	for sql, ndata := range data {
		for table, rawdata := range ndata {
			for typ, item := range rawdata {
				tag := tipb.ResourceGroupTag{
					SqlDigest:  []byte(sql),
					PlanDigest: []byte(sql),
				}
				if typ == "record" {
					tag.Label = tipb.ResourceGroupTagLabel_ResourceGroupTagLabelRow.Enum()
				} else if typ == "index" {
					tag.Label = tipb.ResourceGroupTagLabel_ResourceGroupTagLabelIndex.Enum()
				}
				tid, err := strconv.Atoi(strings.Split(table, "-")[1])
				s.NoError(err)
				tag.TableId = int64(tid)
				var items []*rsmetering.GroupTagRecordItem
				for i := range item.ts {
					items = append(items, &rsmetering.GroupTagRecordItem{
						TimestampSec: item.ts[i],
						CpuTimeMs:    uint32(item.cpu[i]),
						ReadKeys:     uint32(item.reads[i]),
					})
				}

				tagBytes, err := tag.Marshal()
				s.NoError(err)
				s.NoError(s.ds.ResourceMeteringRecord(instance, tikvInstanceType, &rsmetering.ResourceUsageRecord{
					RecordOneof: &rsmetering.ResourceUsageRecord_Record{Record: &rsmetering.GroupTagRecord{
						ResourceGroupTag: tagBytes,
						Items:            items},
					}}, nil))
			}
		}
	}
	vmstorage.Storage.DebugFlush()

	// normal case
	var res []query.SummaryByItem
	err := s.dq.SummaryBy(int(testBaseTs), int(testBaseTs+40), 10, 5, instance, tikvInstanceType, query.AggLevelTable, &res)
	s.NoError(err)
	sortListSeq := []int{1, 2, 0}
	resList := make([]int, 0, len(res))
	for _, item := range res {
		tid, err := strconv.Atoi(item.Text)
		s.NoError(err)
		resList = append(resList, tid)
	}
	s.Equal(sortListSeq, resList)
	res = res[:0]
	err = s.dq.SummaryBy(int(testBaseTs), int(testBaseTs+40), 10, 5, instance, tikvInstanceType, query.AggLevelDB, &res)
	s.NoError(err)
	s.Assert().Len(res, 1)
}
func (s *testTopSQLSuite) TestTiKVSummary() {
	type tikvPlanItem struct {
		ts    []uint64
		cpu   []uint64
		exec  []uint64
		reads []uint64
	}
	type tikvTestPlan map[string]tikvPlanItem
	type tikvTestData map[string]map[string]tikvTestPlan

	instance := "127.0.0.1:20180"
	tikvInstanceType := "tikv"
	tidbInstanceType := "tidb"

	// sql-0:
	//  <unknown>: ts:      testBaseTs+0, testBaseTs+10, testBaseTs+20, testBaseTs+30, testBaseTs+40
	//             cpu:     85, 64, 43, 19, 31
	//             exec:    40, 92, 38, 87, 21
	//             indexes: 11, 69, 58, 21, 56
	//     plan-0: ts:      testBaseTs+0, testBaseTs+10, testBaseTs+20, testBaseTs+30, testBaseTs+40
	//             cpu:     67, 19, 54, 53, 71
	//             exec:    49, 11, 74, 72, 98
	//             rows:    97, 82, 24, 44, 88
	//     plan-0: ts:      testBaseTs+0, testBaseTs+10, testBaseTs+20, testBaseTs+30, testBaseTs+40
	//             cpu:     49, 93, 12, 43, 95
	//             exec:    70, 53, 36, 84, 69
	//             indexes: 80, 44, 12, 10, 40
	// sql-1:
	//     plan-0: ts:      testBaseTs+0, testBaseTs+10, testBaseTs+20, testBaseTs+30, testBaseTs+40
	//             cpu:     97, 46, 29, 22, 35
	//             exec:    68, 86, 24, 70, 75
	//             indexes: 90, 59, 46, 80, 16
	//     plan-1: ts:      testBaseTs+0, testBaseTs+10, testBaseTs+20, testBaseTs+30, testBaseTs+40
	//             cpu:     51, 99, 14, 65, 27
	//             exec:    16, 11, 96, 73, 31
	//             rows:    22, 11, 77, 84, 33
	//     plan-2: ts:      testBaseTs+0, testBaseTs+10, testBaseTs+20, testBaseTs+30, testBaseTs+40
	//             cpu:     61, 64, 83, 99, 43
	//             exec:    98, 61, 98, 33, 92
	//             rows:    51, 93, 42, 27, 21
	// sql-2:
	//     plan-0: ts:      testBaseTs+0, testBaseTs+10, testBaseTs+20, testBaseTs+30, testBaseTs+40
	//             cpu:     61, 87, 37, 55, 53
	//             exec:    54, 98, 46, 35, 52
	//             rows:    50, 46, 19, 63, 81
	data := tikvTestData{
		"sql-0": {
			"": {
				"index": {
					ts:    []uint64{testBaseTs + 0, testBaseTs + 10, testBaseTs + 20, testBaseTs + 30, testBaseTs + 40},
					cpu:   []uint64{85, 64, 43, 19, 31},
					exec:  []uint64{40, 92, 38, 87, 21},
					reads: []uint64{11, 69, 58, 21, 56},
				},
			},
			"plan-0": {
				"record": {
					ts:    []uint64{testBaseTs + 0, testBaseTs + 10, testBaseTs + 20, testBaseTs + 30, testBaseTs + 40},
					cpu:   []uint64{67, 19, 54, 53, 71},
					exec:  []uint64{49, 11, 74, 72, 98},
					reads: []uint64{97, 82, 24, 44, 88},
				},
				"index": {
					ts:    []uint64{testBaseTs + 0, testBaseTs + 10, testBaseTs + 20, testBaseTs + 30, testBaseTs + 40},
					cpu:   []uint64{49, 93, 12, 43, 95},
					exec:  []uint64{70, 53, 36, 84, 69},
					reads: []uint64{80, 44, 12, 10, 40},
				},
			},
		},
		"sql-1": {
			"plan-0": {
				"index": {
					ts:    []uint64{testBaseTs + 0, testBaseTs + 10, testBaseTs + 20, testBaseTs + 30, testBaseTs + 40},
					cpu:   []uint64{97, 46, 29, 22, 35},
					exec:  []uint64{68, 86, 24, 70, 75},
					reads: []uint64{90, 59, 46, 80, 16},
				},
			},
			"plan-1": {
				"record": {
					ts:    []uint64{testBaseTs + 0, testBaseTs + 10, testBaseTs + 20, testBaseTs + 30, testBaseTs + 40},
					cpu:   []uint64{51, 99, 14, 65, 27},
					exec:  []uint64{16, 11, 96, 73, 31},
					reads: []uint64{22, 11, 77, 84, 33},
				},
			},
			"plan-2": {
				"record": {
					ts:    []uint64{testBaseTs + 0, testBaseTs + 10, testBaseTs + 20, testBaseTs + 30, testBaseTs + 40},
					cpu:   []uint64{61, 64, 83, 99, 43},
					exec:  []uint64{98, 61, 98, 33, 92},
					reads: []uint64{51, 93, 42, 27, 21},
				},
			},
		},
		"sql-2": {
			"plan-0": {
				"record": {
					ts:    []uint64{testBaseTs + 0, testBaseTs + 10, testBaseTs + 20, testBaseTs + 30, testBaseTs + 40},
					cpu:   []uint64{61, 87, 37, 55, 53},
					exec:  []uint64{54, 98, 46, 35, 52},
					reads: []uint64{50, 46, 19, 63, 81},
				},
			},
		},
	}

	for sqlDigest, plans := range data {
		for planDigest, reads := range plans {
			for typ, plan := range reads {
				tag := tipb.ResourceGroupTag{
					SqlDigest:  []byte(sqlDigest),
					PlanDigest: []byte(planDigest),
				}
				if typ == "record" {
					tag.Label = tipb.ResourceGroupTagLabel_ResourceGroupTagLabelRow.Enum()
				} else if typ == "index" {
					tag.Label = tipb.ResourceGroupTagLabel_ResourceGroupTagLabelIndex.Enum()
				}
				var items []*rsmetering.GroupTagRecordItem
				var tidbItems []*tipb.TopSQLRecordItem
				for i := range plan.ts {
					items = append(items, &rsmetering.GroupTagRecordItem{
						TimestampSec: plan.ts[i],
						CpuTimeMs:    uint32(plan.cpu[i]),
						ReadKeys:     uint32(plan.reads[i]),
					})
					tidbItems = append(tidbItems, &tipb.TopSQLRecordItem{
						TimestampSec: plan.ts[i],
						StmtKvExecCount: map[string]uint64{
							instance: plan.exec[i],
						},
					})
				}

				tagBytes, err := tag.Marshal()
				s.NoError(err)
				s.NoError(s.ds.ResourceMeteringRecord(instance, tikvInstanceType, &rsmetering.ResourceUsageRecord{
					RecordOneof: &rsmetering.ResourceUsageRecord_Record{Record: &rsmetering.GroupTagRecord{
						ResourceGroupTag: tagBytes,
						Items:            items},
					}}, nil))
				s.NoError(s.ds.TopSQLRecord(instance, tidbInstanceType, &tipb.TopSQLRecord{
					SqlDigest:  []byte(sqlDigest),
					PlanDigest: []byte(planDigest),
					Items:      tidbItems,
				}))
			}
		}
	}

	vmstorage.Storage.DebugFlush()

	// normal case
	var res []query.SummaryItem
	err := s.dq.Summary(int(testBaseTs), int(testBaseTs+40), 10, 5, instance, tikvInstanceType, &res)
	s.NoError(err)
	s.sortSummary(res)
	tsList := []uint64{testBaseTs + 0, testBaseTs + 10, testBaseTs + 20, testBaseTs + 30, testBaseTs + 40}
	timeWindow := float64(40 + 1)
	s.Equal(res, []query.SummaryItem{{
		SQLDigest:         hex.EncodeToString([]byte("sql-0")),
		CPUTimeMs:         sum(data["sql-0"][""]["index"].cpu, data["sql-0"]["plan-0"]["record"].cpu, data["sql-0"]["plan-0"]["index"].cpu),
		ExecCountPerSec:   sumf(data["sql-0"][""]["index"].exec, data["sql-0"]["plan-0"]["record"].exec, data["sql-0"]["plan-0"]["index"].exec) / timeWindow,
		ScanRecordsPerSec: sumf(data["sql-0"]["plan-0"]["record"].reads) / timeWindow,
		ScanIndexesPerSec: sumf(data["sql-0"][""]["index"].reads, data["sql-0"]["plan-0"]["index"].reads) / timeWindow,
		Plans: []query.SummaryPlanItem{{
			TimestampSec:      tsList,
			CPUTimeMs:         data["sql-0"][""]["index"].cpu,
			ExecCountPerSec:   sumf(data["sql-0"][""]["index"].exec) / timeWindow,
			ScanIndexesPerSec: sumf(data["sql-0"][""]["index"].reads) / timeWindow,
		}, {
			PlanDigest:        hex.EncodeToString([]byte("plan-0")),
			TimestampSec:      tsList,
			CPUTimeMs:         sumv(data["sql-0"]["plan-0"]["record"].cpu, data["sql-0"]["plan-0"]["index"].cpu),
			ExecCountPerSec:   sumf(data["sql-0"]["plan-0"]["record"].exec, data["sql-0"]["plan-0"]["index"].exec) / timeWindow,
			ScanRecordsPerSec: sumf(data["sql-0"]["plan-0"]["record"].reads) / timeWindow,
			ScanIndexesPerSec: sumf(data["sql-0"]["plan-0"]["index"].reads) / timeWindow,
		}},
	}, {
		SQLDigest:         hex.EncodeToString([]byte("sql-1")),
		CPUTimeMs:         sum(data["sql-1"]["plan-0"]["index"].cpu, data["sql-1"]["plan-1"]["record"].cpu, data["sql-1"]["plan-2"]["record"].cpu),
		ExecCountPerSec:   sumf(data["sql-1"]["plan-0"]["index"].exec, data["sql-1"]["plan-1"]["record"].exec, data["sql-1"]["plan-2"]["record"].exec) / timeWindow,
		ScanRecordsPerSec: sumf(data["sql-1"]["plan-1"]["record"].reads, data["sql-1"]["plan-2"]["record"].reads) / timeWindow,
		ScanIndexesPerSec: sumf(data["sql-1"]["plan-0"]["index"].reads) / timeWindow,
		Plans: []query.SummaryPlanItem{{
			PlanDigest:        hex.EncodeToString([]byte("plan-0")),
			TimestampSec:      tsList,
			CPUTimeMs:         data["sql-1"]["plan-0"]["index"].cpu,
			ExecCountPerSec:   sumf(data["sql-1"]["plan-0"]["index"].exec) / timeWindow,
			ScanIndexesPerSec: sumf(data["sql-1"]["plan-0"]["index"].reads) / timeWindow,
		}, {
			PlanDigest:        hex.EncodeToString([]byte("plan-1")),
			TimestampSec:      tsList,
			CPUTimeMs:         data["sql-1"]["plan-1"]["record"].cpu,
			ExecCountPerSec:   sumf(data["sql-1"]["plan-1"]["record"].exec) / timeWindow,
			ScanRecordsPerSec: sumf(data["sql-1"]["plan-1"]["record"].reads) / timeWindow,
		}, {
			PlanDigest:        hex.EncodeToString([]byte("plan-2")),
			TimestampSec:      tsList,
			CPUTimeMs:         data["sql-1"]["plan-2"]["record"].cpu,
			ExecCountPerSec:   sumf(data["sql-1"]["plan-2"]["record"].exec) / timeWindow,
			ScanRecordsPerSec: sumf(data["sql-1"]["plan-2"]["record"].reads) / timeWindow,
		}},
	}, {
		SQLDigest:         hex.EncodeToString([]byte("sql-2")),
		CPUTimeMs:         sum(data["sql-2"]["plan-0"]["record"].cpu),
		ExecCountPerSec:   sumf(data["sql-2"]["plan-0"]["record"].exec) / timeWindow,
		ScanRecordsPerSec: sumf(data["sql-2"]["plan-0"]["record"].reads) / timeWindow,
		Plans: []query.SummaryPlanItem{{
			PlanDigest:        hex.EncodeToString([]byte("plan-0")),
			TimestampSec:      tsList,
			CPUTimeMs:         data["sql-2"]["plan-0"]["record"].cpu,
			ExecCountPerSec:   sumf(data["sql-2"]["plan-0"]["record"].exec) / timeWindow,
			ScanRecordsPerSec: sumf(data["sql-2"]["plan-0"]["record"].reads) / timeWindow,
		}},
	}})

	// top 1
	res = nil
	err = s.dq.Summary(int(testBaseTs), int(testBaseTs+40), 10, 1, instance, tikvInstanceType, &res)
	s.NoError(err)
	s.sortSummary(res)
	tsList = []uint64{testBaseTs + 0, testBaseTs + 10, testBaseTs + 20, testBaseTs + 30, testBaseTs + 40}
	timeWindow = float64(40 + 1)
	s.Equal(res, []query.SummaryItem{{
		IsOther: true,
		CPUTimeMs: sum(
			data["sql-0"][""]["index"].cpu,
			data["sql-0"]["plan-0"]["record"].cpu,
			data["sql-0"]["plan-0"]["index"].cpu,
			data["sql-2"]["plan-0"]["record"].cpu,
		),
		ExecCountPerSec: sumf(
			data["sql-0"][""]["index"].exec,
			data["sql-0"]["plan-0"]["record"].exec,
			data["sql-0"]["plan-0"]["index"].exec,
			data["sql-2"]["plan-0"]["record"].exec,
		) / timeWindow,
		ScanRecordsPerSec: sumf(
			data["sql-0"]["plan-0"]["record"].reads,
			data["sql-2"]["plan-0"]["record"].reads,
		) / timeWindow,
		ScanIndexesPerSec: sumf(
			data["sql-0"][""]["index"].reads,
			data["sql-0"]["plan-0"]["index"].reads,
		) / timeWindow,
		Plans: []query.SummaryPlanItem{{
			TimestampSec: tsList,
			CPUTimeMs: sumv(
				data["sql-0"][""]["index"].cpu,
				data["sql-0"]["plan-0"]["record"].cpu,
				data["sql-0"]["plan-0"]["index"].cpu,
				data["sql-2"]["plan-0"]["record"].cpu,
			),
			ExecCountPerSec: sumf(
				data["sql-0"][""]["index"].exec,
				data["sql-0"]["plan-0"]["record"].exec,
				data["sql-0"]["plan-0"]["index"].exec,
				data["sql-2"]["plan-0"]["record"].exec,
			) / timeWindow,
			ScanRecordsPerSec: sumf(
				data["sql-0"]["plan-0"]["record"].reads,
				data["sql-2"]["plan-0"]["record"].reads,
			) / timeWindow,
			ScanIndexesPerSec: sumf(
				data["sql-0"][""]["index"].reads,
				data["sql-0"]["plan-0"]["index"].reads,
			) / timeWindow,
		}},
	}, {
		SQLDigest:         hex.EncodeToString([]byte("sql-1")),
		CPUTimeMs:         sum(data["sql-1"]["plan-0"]["index"].cpu, data["sql-1"]["plan-1"]["record"].cpu, data["sql-1"]["plan-2"]["record"].cpu),
		ExecCountPerSec:   sumf(data["sql-1"]["plan-0"]["index"].exec, data["sql-1"]["plan-1"]["record"].exec, data["sql-1"]["plan-2"]["record"].exec) / timeWindow,
		ScanRecordsPerSec: sumf(data["sql-1"]["plan-1"]["record"].reads, data["sql-1"]["plan-2"]["record"].reads) / timeWindow,
		ScanIndexesPerSec: sumf(data["sql-1"]["plan-0"]["index"].reads) / timeWindow,
		Plans: []query.SummaryPlanItem{{
			PlanDigest:        hex.EncodeToString([]byte("plan-0")),
			TimestampSec:      tsList,
			CPUTimeMs:         data["sql-1"]["plan-0"]["index"].cpu,
			ExecCountPerSec:   sumf(data["sql-1"]["plan-0"]["index"].exec) / timeWindow,
			ScanIndexesPerSec: sumf(data["sql-1"]["plan-0"]["index"].reads) / timeWindow,
		}, {
			PlanDigest:        hex.EncodeToString([]byte("plan-1")),
			TimestampSec:      tsList,
			CPUTimeMs:         data["sql-1"]["plan-1"]["record"].cpu,
			ExecCountPerSec:   sumf(data["sql-1"]["plan-1"]["record"].exec) / timeWindow,
			ScanRecordsPerSec: sumf(data["sql-1"]["plan-1"]["record"].reads) / timeWindow,
		}, {
			PlanDigest:        hex.EncodeToString([]byte("plan-2")),
			TimestampSec:      tsList,
			CPUTimeMs:         data["sql-1"]["plan-2"]["record"].cpu,
			ExecCountPerSec:   sumf(data["sql-1"]["plan-2"]["record"].exec) / timeWindow,
			ScanRecordsPerSec: sumf(data["sql-1"]["plan-2"]["record"].reads) / timeWindow,
		}},
	}})

	// no data
	res = nil
	err = s.dq.Summary(int(testBaseTs+41), int(testBaseTs+100), 10, 5, instance, tikvInstanceType, &res)
	s.NoError(err)
	s.Equal(len(res), 0)

	// one point
	res = nil
	err = s.dq.Summary(int(testBaseTs+40), int(testBaseTs+40), 10, 5, instance, tikvInstanceType, &res)
	s.NoError(err)
	s.sortSummary(res)
	tsList = []uint64{testBaseTs + 40}
	timeWindow = float64(1)
	s.Equal(res, []query.SummaryItem{{
		SQLDigest: hex.EncodeToString([]byte("sql-0")),
		CPUTimeMs: sum(
			data["sql-0"][""]["index"].cpu[4:],
			data["sql-0"]["plan-0"]["record"].cpu[4:],
			data["sql-0"]["plan-0"]["index"].cpu[4:],
		),
		ExecCountPerSec: sumf(
			data["sql-0"][""]["index"].exec[4:],
			data["sql-0"]["plan-0"]["record"].exec[4:],
			data["sql-0"]["plan-0"]["index"].exec[4:],
		) / timeWindow,
		ScanRecordsPerSec: sumf(
			data["sql-0"]["plan-0"]["record"].reads[4:],
		) / timeWindow,
		ScanIndexesPerSec: sumf(
			data["sql-0"][""]["index"].reads[4:],
			data["sql-0"]["plan-0"]["index"].reads[4:],
		) / timeWindow,
		Plans: []query.SummaryPlanItem{{
			TimestampSec:      tsList,
			CPUTimeMs:         data["sql-0"][""]["index"].cpu[4:],
			ExecCountPerSec:   sumf(data["sql-0"][""]["index"].exec[4:]) / timeWindow,
			ScanIndexesPerSec: sumf(data["sql-0"][""]["index"].reads[4:]) / timeWindow,
		}, {
			PlanDigest:        hex.EncodeToString([]byte("plan-0")),
			TimestampSec:      tsList,
			CPUTimeMs:         sumv(data["sql-0"]["plan-0"]["record"].cpu[4:], data["sql-0"]["plan-0"]["index"].cpu[4:]),
			ExecCountPerSec:   sumf(data["sql-0"]["plan-0"]["record"].exec[4:], data["sql-0"]["plan-0"]["index"].exec[4:]) / timeWindow,
			ScanRecordsPerSec: sumf(data["sql-0"]["plan-0"]["record"].reads[4:]) / timeWindow,
			ScanIndexesPerSec: sumf(data["sql-0"]["plan-0"]["index"].reads[4:]) / timeWindow,
		}},
	}, {
		SQLDigest:         hex.EncodeToString([]byte("sql-1")),
		CPUTimeMs:         sum(data["sql-1"]["plan-0"]["index"].cpu[4:], data["sql-1"]["plan-1"]["record"].cpu[4:], data["sql-1"]["plan-2"]["record"].cpu[4:]),
		ExecCountPerSec:   sumf(data["sql-1"]["plan-0"]["index"].exec[4:], data["sql-1"]["plan-1"]["record"].exec[4:], data["sql-1"]["plan-2"]["record"].exec[4:]) / timeWindow,
		ScanRecordsPerSec: sumf(data["sql-1"]["plan-1"]["record"].reads[4:], data["sql-1"]["plan-2"]["record"].reads[4:]) / timeWindow,
		ScanIndexesPerSec: sumf(data["sql-1"]["plan-0"]["index"].reads[4:]) / timeWindow,
		Plans: []query.SummaryPlanItem{{
			PlanDigest:        hex.EncodeToString([]byte("plan-0")),
			TimestampSec:      tsList,
			CPUTimeMs:         data["sql-1"]["plan-0"]["index"].cpu[4:],
			ExecCountPerSec:   sumf(data["sql-1"]["plan-0"]["index"].exec[4:]) / timeWindow,
			ScanIndexesPerSec: sumf(data["sql-1"]["plan-0"]["index"].reads[4:]) / timeWindow,
		}, {
			PlanDigest:        hex.EncodeToString([]byte("plan-1")),
			TimestampSec:      tsList,
			CPUTimeMs:         data["sql-1"]["plan-1"]["record"].cpu[4:],
			ExecCountPerSec:   sumf(data["sql-1"]["plan-1"]["record"].exec[4:]) / timeWindow,
			ScanRecordsPerSec: sumf(data["sql-1"]["plan-1"]["record"].reads[4:]) / timeWindow,
		}, {
			PlanDigest:        hex.EncodeToString([]byte("plan-2")),
			TimestampSec:      tsList,
			CPUTimeMs:         data["sql-1"]["plan-2"]["record"].cpu[4:],
			ExecCountPerSec:   sumf(data["sql-1"]["plan-2"]["record"].exec[4:]) / timeWindow,
			ScanRecordsPerSec: sumf(data["sql-1"]["plan-2"]["record"].reads[4:]) / timeWindow,
		}},
	}, {
		SQLDigest:         hex.EncodeToString([]byte("sql-2")),
		CPUTimeMs:         sum(data["sql-2"]["plan-0"]["record"].cpu[4:]),
		ExecCountPerSec:   sumf(data["sql-2"]["plan-0"]["record"].exec[4:]) / timeWindow,
		ScanRecordsPerSec: sumf(data["sql-2"]["plan-0"]["record"].reads[4:]) / timeWindow,
		Plans: []query.SummaryPlanItem{{
			PlanDigest:        hex.EncodeToString([]byte("plan-0")),
			TimestampSec:      tsList,
			CPUTimeMs:         data["sql-2"]["plan-0"]["record"].cpu[4:],
			ExecCountPerSec:   sumf(data["sql-2"]["plan-0"]["record"].exec[4:]) / timeWindow,
			ScanRecordsPerSec: sumf(data["sql-2"]["plan-0"]["record"].reads[4:]) / timeWindow,
		}},
	}})

	// two points
	res = nil
	err = s.dq.Summary(int(testBaseTs+19), int(testBaseTs+32), 10, 5, instance, tikvInstanceType, &res)
	s.NoError(err)
	s.sortSummary(res)
	tsList = []uint64{testBaseTs + 22, testBaseTs + 32}
	timeWindow = float64(32 - 19 + 1)
	s.Equal(res, []query.SummaryItem{{
		SQLDigest: hex.EncodeToString([]byte("sql-0")),
		CPUTimeMs: sum(
			data["sql-0"][""]["index"].cpu[2:4],
			data["sql-0"]["plan-0"]["record"].cpu[2:4],
			data["sql-0"]["plan-0"]["index"].cpu[2:4],
		),
		ExecCountPerSec: sumf(
			data["sql-0"][""]["index"].exec[2:4],
			data["sql-0"]["plan-0"]["record"].exec[2:4],
			data["sql-0"]["plan-0"]["index"].exec[2:4],
		) / timeWindow,
		ScanRecordsPerSec: sumf(data["sql-0"]["plan-0"]["record"].reads[2:4]) / timeWindow,
		ScanIndexesPerSec: sumf(data["sql-0"][""]["index"].reads[2:4], data["sql-0"]["plan-0"]["index"].reads[2:4]) / timeWindow,
		Plans: []query.SummaryPlanItem{{
			TimestampSec:      tsList,
			CPUTimeMs:         data["sql-0"][""]["index"].cpu[2:4],
			ExecCountPerSec:   sumf(data["sql-0"][""]["index"].exec[2:4]) / timeWindow,
			ScanIndexesPerSec: sumf(data["sql-0"][""]["index"].reads[2:4]) / timeWindow,
		}, {
			PlanDigest:        hex.EncodeToString([]byte("plan-0")),
			TimestampSec:      tsList,
			CPUTimeMs:         sumv(data["sql-0"]["plan-0"]["record"].cpu[2:4], data["sql-0"]["plan-0"]["index"].cpu[2:4]),
			ExecCountPerSec:   sumf(data["sql-0"]["plan-0"]["record"].exec[2:4], data["sql-0"]["plan-0"]["index"].exec[2:4]) / timeWindow,
			ScanRecordsPerSec: sumf(data["sql-0"]["plan-0"]["record"].reads[2:4]) / timeWindow,
			ScanIndexesPerSec: sumf(data["sql-0"]["plan-0"]["index"].reads[2:4]) / timeWindow,
		}},
	}, {
		SQLDigest: hex.EncodeToString([]byte("sql-1")),
		CPUTimeMs: sum(
			data["sql-1"]["plan-0"]["index"].cpu[2:4],
			data["sql-1"]["plan-1"]["record"].cpu[2:4],
			data["sql-1"]["plan-2"]["record"].cpu[2:4],
		),
		ExecCountPerSec: sumf(
			data["sql-1"]["plan-0"]["index"].exec[2:4],
			data["sql-1"]["plan-1"]["record"].exec[2:4],
			data["sql-1"]["plan-2"]["record"].exec[2:4],
		) / timeWindow,
		ScanRecordsPerSec: sumf(data["sql-1"]["plan-1"]["record"].reads[2:4], data["sql-1"]["plan-2"]["record"].reads[2:4]) / timeWindow,
		ScanIndexesPerSec: sumf(data["sql-1"]["plan-0"]["index"].reads[2:4]) / timeWindow,
		Plans: []query.SummaryPlanItem{{
			PlanDigest:        hex.EncodeToString([]byte("plan-0")),
			TimestampSec:      tsList,
			CPUTimeMs:         data["sql-1"]["plan-0"]["index"].cpu[2:4],
			ExecCountPerSec:   sumf(data["sql-1"]["plan-0"]["index"].exec[2:4]) / timeWindow,
			ScanIndexesPerSec: sumf(data["sql-1"]["plan-0"]["index"].reads[2:4]) / timeWindow,
		}, {
			PlanDigest:        hex.EncodeToString([]byte("plan-1")),
			TimestampSec:      tsList,
			CPUTimeMs:         data["sql-1"]["plan-1"]["record"].cpu[2:4],
			ExecCountPerSec:   sumf(data["sql-1"]["plan-1"]["record"].exec[2:4]) / timeWindow,
			ScanRecordsPerSec: sumf(data["sql-1"]["plan-1"]["record"].reads[2:4]) / timeWindow,
		}, {
			PlanDigest:        hex.EncodeToString([]byte("plan-2")),
			TimestampSec:      tsList,
			CPUTimeMs:         data["sql-1"]["plan-2"]["record"].cpu[2:4],
			ExecCountPerSec:   sumf(data["sql-1"]["plan-2"]["record"].exec[2:4]) / timeWindow,
			ScanRecordsPerSec: sumf(data["sql-1"]["plan-2"]["record"].reads[2:4]) / timeWindow,
		}},
	}, {
		SQLDigest:         hex.EncodeToString([]byte("sql-2")),
		CPUTimeMs:         sum(data["sql-2"]["plan-0"]["record"].cpu[2:4]),
		ExecCountPerSec:   sumf(data["sql-2"]["plan-0"]["record"].exec[2:4]) / timeWindow,
		ScanRecordsPerSec: sumf(data["sql-2"]["plan-0"]["record"].reads[2:4]) / timeWindow,
		Plans: []query.SummaryPlanItem{{
			PlanDigest:        hex.EncodeToString([]byte("plan-0")),
			TimestampSec:      tsList,
			CPUTimeMs:         data["sql-2"]["plan-0"]["record"].cpu[2:4],
			ExecCountPerSec:   sumf(data["sql-2"]["plan-0"]["record"].exec[2:4]) / timeWindow,
			ScanRecordsPerSec: sumf(data["sql-2"]["plan-0"]["record"].reads[2:4]) / timeWindow,
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

func sum(s ...[]uint64) uint64 {
	sum := uint64(0)
	for _, items := range s {
		for _, item := range items {
			sum += item
		}
	}
	return sum
}

func sumf(s ...[]uint64) float64 {
	return float64(sum(s...))
}

func sumv(v ...[]uint64) []uint64 {
	res := make([]uint64, 0, len(v))
	for i := range v[0] {
		res = append(res, 0)
		for _, s := range v {
			res[len(res)-1] += s[i]
		}
	}
	return res
}
