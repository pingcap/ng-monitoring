package topsql_test

import (
	"context"

	"github.com/pingcap/ng-monitoring/component/topsql/query"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmstorage"
	"github.com/pingcap/tipb/go-tipb"
)

// The cross-instance aggregator writes contributions that arrive after a second was
// flushed as an extra sample with the same timestamp. That is only correct while the
// embedded VictoriaMetrics keeps duplicate-timestamp samples (dedup off) and the
// read paths sum them, including after background merges. Enabling dedup breaks
// the aggregate instance; this test pins the behaviour.
func (s *testTopSQLSuite) TestDuplicateTimestampSamplesAreSummed() {
	instance := "127.0.0.1:19995"
	record := func(cpu uint32) *tipb.TopSQLRecord {
		return &tipb.TopSQLRecord{
			SqlDigest:  []byte("sql-dup-ts"),
			PlanDigest: []byte("plan-dup-ts"),
			Items:      []*tipb.TopSQLRecordItem{{TimestampSec: testBaseTs + 1, CpuTimeMs: cpu, StmtExecCount: 1}},
		}
	}
	s.NoError(s.ds.TopSQLRecord(instance, "tidb", record(10)))
	vmstorage.Storage.DebugFlush()
	s.NoError(s.ds.TopSQLRecord(instance, "tidb", record(20)))
	vmstorage.Storage.DebugFlush()

	check := func() {
		var res []query.SummaryItem
		s.NoError(s.dq.Summary(context.Background(), int(testBaseTs), int(testBaseTs+10), 10, 5, instance, "tidb", &res, query.OrderByCPU))
		s.Require().Len(res, 1)
		s.Equal(uint64(30), res[0].CPUTimeMs)
		s.InDelta(2.0/11.0, res[0].ExecCountPerSec, 1e-9)
	}
	check()

	s.NoError(vmstorage.Storage.ForceMergePartitions(""))
	vmstorage.Storage.DebugFlush()
	check()
}
