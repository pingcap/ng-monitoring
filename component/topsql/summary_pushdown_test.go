package topsql_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	"github.com/pingcap/ng-monitoring/component/topsql/query"
	"github.com/pingcap/ng-monitoring/database/timeseries"
	"github.com/pingcap/ng-monitoring/utils"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/promql"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/searchutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/querytracer"
)

func (s *testTopSQLSuite) TestSummaryCompositeIncrementalAggregation() {
	for _, tc := range []struct {
		order, instanceType, in, out string
	}{
		{query.OrderByNetwork, "tikv", "network_in_bytes", "network_out_bytes"},
		{query.OrderByNetwork, "tidb", "sql_network_in_bytes", "sql_network_out_bytes"},
		{query.OrderByLogicalIO, "tikv", "logical_read_bytes", "logical_write_bytes"},
	} {
		s.Run(tc.order+"-"+tc.instanceType, func() {
			instance := "incremental-" + tc.order + "-" + tc.instanceType
			line := func(metric, sql, plan, table string, offset, value int) string {
				return fmt.Sprintf("%s{instance=%q,instance_type=%q,sql_digest=%q,plan_digest=%q,table=%q} %d %d\n", metric, instance, tc.instanceType, sql, plan, table, value, (testBaseTs+uint64(offset))*1000)
			}
			// Include disjoint tables, a plan with only one side, different sample
			// windows, and native Other. No raw-series vector join is necessary.
			s.importSummarySamples(line(tc.in, "aa", "11", "t1", 0, 100) +
				line(tc.out, "aa", "11", "t1", 6, 7) +
				line(tc.in, "aa", "11", "t2", 0, 3) +
				line(tc.out, "aa", "11", "t3", 0, 5) +
				line(tc.out, "aa", "22", "t1", 0, 11) +
				line(tc.in, "bb", "11", "t1", 0, 2) +
				line(tc.out, "bb", "11", "t1", 0, 2) +
				line(tc.in, "", "", "", 0, 2))
			mainQueries := 0
			var mainExpressions []string
			dq := query.NewDefaultQuery(func(w http.ResponseWriter, r *http.Request) {
				expr := r.URL.Query().Get("query")
				main := strings.Contains(expr, tc.in+"{")
				if main {
					q := r.URL.Query()
					q.Set("trace", "1")
					r.URL.RawQuery = q.Encode()
				}
				timeseries.SelectHandler(w, r)
				if main {
					mainQueries++
					mainExpressions = append(mainExpressions, expr)
					trace := w.(*utils.ResponseWriter).Body.String()
					s.GreaterOrEqual(strings.Count(trace, "with incremental aggregation sum()"), 2, "both metric scans must aggregate incrementally: %s", trace)
				}
			}, s.docDB)
			var items []query.SummaryItem
			s.Require().NoError(dq.Summary(context.Background(), int(testBaseTs), int(testBaseTs+6), 6, 1, instance, tc.instanceType, &items, tc.order))
			s.Require().Equal(3, mainQueries, "ranking, selected curve and Other")
			s.sortSummary(items)
			s.Require().Len(items, 2)
			s.True(items[0].IsOther)
			s.Equal("aa", items[1].SQLDigest)
			usage := func(item query.SummaryItem) uint64 {
				if tc.order == query.OrderByNetwork {
					return item.NetworkBytes
				}
				return item.LogicalIoBytes
			}
			s.Equal(uint64(6), usage(items[0]))
			s.Equal(uint64(126), usage(items[1]))
			s.Require().Len(items[1].Plans, 2)
			for _, plan := range items[1].Plans {
				values := plan.NetworkBytes
				if tc.order == query.OrderByLogicalIO {
					values = plan.LogicalIoBytes
				}
				if plan.PlanDigest == "11" {
					s.Equal([]uint64{testBaseTs, testBaseTs + 6}, plan.TimestampSec)
					s.Equal([]uint64{108, 7}, values)
				} else {
					s.Equal("22", plan.PlanDigest)
					s.Equal([]uint64{11}, values)
				}
			}
			// Each metric has at most four raw series. Keep those scan budgets
			// independent: the combined selector's eight series exceed the same
			// limit even though both approaches produce few aggregate results.
			exec := func(expr string) error {
				ec := &promql.EvalConfig{
					Start: int64(testBaseTs) * 1000, End: int64(testBaseTs+6) * 1000, Step: 6000,
					MaxSeries: 4, MaxPointsPerSeries: 10, RoundDigits: 100,
					Deadline: searchutil.NewDeadline(time.Now(), 5*time.Second, "test"),
				}
				_, err := promql.Exec(querytracer.New(false, "scan limit"), ec, expr, false)
				return err
			}
			for _, expr := range mainExpressions {
				s.NoError(exec(expr))
			}
			combined := fmt.Sprintf("sum(sum_over_time({__name__=~%q,instance=%q,instance_type=%q}[12]))", tc.in+"|"+tc.out, instance, tc.instanceType)
			s.ErrorContains(exec(combined), "number of matching timeseries exceeds 4", "a combined metric selector would lower the effective per-family limit")
		})
	}
}

func (s *testTopSQLSuite) importSummarySamples(lines string) {
	req := httptest.NewRequest(http.MethodPost, "/api/v1/import/prometheus", strings.NewReader(lines))
	w := httptest.NewRecorder()
	timeseries.InsertHandler(w, req)
	s.Require().Equal(http.StatusNoContent, w.Code, w.Body.String())
	vmstorage.Storage.DebugFlush()
}

func (s *testTopSQLSuite) TestSummaryUnlimitedTableAggregation() {
	for _, tc := range []struct{ order, metric string }{
		{query.OrderByCPU, "cpu_time"},
		{query.OrderByNetwork, "network_in_bytes"},
		{query.OrderByLogicalIO, "logical_read_bytes"},
	} {
		s.Run(tc.order, func() {
			instance := "unlimited-tables-" + tc.order
			line := func(sql, plan, table string, offset, value int) string {
				return fmt.Sprintf("%s{instance=%q,instance_type=\"tikv\",sql_digest=%q,plan_digest=%q,table=%q} %d %d\n", tc.metric, instance, sql, plan, table, value, (testBaseTs+uint64(offset))*1000)
			}
			s.importSummarySamples(line("aa", "11", "t1", 0, 10) + line("aa", "11", "t2", 0, 20) +
				line("aa", "11", "t1", 6, 3) + line("aa", "11", "t2", 6, 4) +
				line("aa", "22", "t1", 0, 5) + line("", "", "", 0, 2))
			for _, top := range []int{0, -1} {
				var items []query.SummaryItem
				s.Require().NoError(s.dq.Summary(context.Background(), int(testBaseTs), int(testBaseTs+6), 6, top, instance, "tikv", &items, tc.order))
				s.sortSummary(items)
				s.Require().Len(items, 2)
				s.True(items[0].IsOther)
				s.Equal("aa", items[1].SQLDigest)
				s.Require().Len(items[1].Plans, 2)
				for _, plan := range items[1].Plans {
					values := plan.CPUTimeMs
					if tc.order == query.OrderByNetwork {
						values = plan.NetworkBytes
					} else if tc.order == query.OrderByLogicalIO {
						values = plan.LogicalIoBytes
					}
					if plan.PlanDigest == "11" {
						s.Equal([]uint64{testBaseTs, testBaseTs + 6}, plan.TimestampSec)
						s.Equal([]uint64{30, 7}, values, "one point per timestamp after summing tables")
					} else {
						s.Equal("22", plan.PlanDigest)
						s.Equal([]uint64{5}, values)
					}
				}
			}
		})
	}
}

func (s *testTopSQLSuite) TestSummaryLateSamplesKeepInitialSelection() {
	// Ranking is a separate read. Samples arriving afterwards update the selected
	// curves and Other, but do not change the SQL selection during this request.
	for _, initiallyEmpty := range []bool{false, true} {
		s.Run(fmt.Sprintf("empty=%t", initiallyEmpty), func() {
			instance := fmt.Sprintf("late-samples-%t", initiallyEmpty)
			line := func(sql, plan string, value int) string {
				return fmt.Sprintf("cpu_time{instance=%q,instance_type=\"tikv\",sql_digest=%q,plan_digest=%q} %d %d\n", instance, sql, plan, value, testBaseTs*1000)
			}
			initial := line("", "", 2)
			if !initiallyEmpty {
				initial += line("aa", "11", 100)
			}
			s.importSummarySamples(initial)
			rankings := 0
			dq := query.NewDefaultQuery(func(w http.ResponseWriter, r *http.Request) {
				timeseries.SelectHandler(w, r)
				if strings.HasPrefix(r.URL.Query().Get("query"), "topk(") {
					rankings++
					// New high-scoring SQL and a new plan of the previously selected SQL.
					s.importSummarySamples(line("bb", "11", 1000) + line("aa", "22", 50))
				}
			}, s.docDB)
			var items []query.SummaryItem
			s.Require().NoError(dq.Summary(context.Background(), int(testBaseTs), int(testBaseTs), 6, 1, instance, "tikv", &items, query.OrderByCPU))
			s.Equal(1, rankings)
			s.sortSummary(items)
			if initiallyEmpty {
				s.Require().Len(items, 1)
				s.True(items[0].IsOther)
				s.Equal(uint64(1052), items[0].CPUTimeMs)
			} else {
				s.Require().Len(items, 2)
				s.True(items[0].IsOther)
				s.Equal(uint64(1002), items[0].CPUTimeMs)
				s.Equal("aa", items[1].SQLDigest)
				s.Equal(uint64(150), items[1].CPUTimeMs)
				s.Len(items[1].Plans, 2)
			}
		})
	}
}

func (s *testTopSQLSuite) TestSummaryAuxiliaryAggregation() {
	instance := "auxiliary-aggregation"
	line := func(metric, sql, plan, table string, value int) string {
		return fmt.Sprintf("%s{instance=%q,instance_type=\"tikv\",sql_digest=%q,plan_digest=%q,table=%q} %d %d\n", metric, instance, sql, plan, table, value, testBaseTs*1000)
	}
	var lines strings.Builder
	for _, row := range []struct {
		sql, plan, table                 string
		cpu, duration, count, exec, rows int
	}{
		{"aa", "11", "t1", 10, 10000000, 1, 3, 10},
		{"aa", "11", "t2", 20, 90000000, 3, 7, 20},
		{"aa", "22", "t1", 40, 200000000, 2, 11, 40},
		{"aa", "33", "t1", 0, 60000000, 2, 13, 50}, // auxiliary-only plan
		{"bb", "11", "t1", 5, 40000000, 4, 17, 60},
		{"", "", "", 3, 20000000, 1, 19, 70}, // original Other
	} {
		if row.cpu > 0 {
			lines.WriteString(line("cpu_time", row.sql, row.plan, row.table, row.cpu))
		}
		for metric, value := range map[string]int{
			"sql_duration_sum": row.duration, "sql_duration_count": row.count,
			"sql_exec_count": row.exec, "read_row": row.rows,
		} {
			lines.WriteString(line(metric, row.sql, row.plan, row.table, value))
		}
	}
	s.importSummarySamples(lines.String())
	for _, top := range []int{1, 0} {
		var items []query.SummaryItem
		s.Require().NoError(s.dq.Summary(context.Background(), int(testBaseTs), int(testBaseTs), 6, top, instance, "tikv", &items, query.OrderByCPU))
		var selected, other *query.SummaryItem
		var exec, rows float64
		for i := range items {
			item := &items[i]
			exec += item.ExecCountPerSec
			rows += item.ScanRecordsPerSec
			if item.SQLDigest == "aa" {
				selected = item
			}
			if item.IsOther {
				other = item
			}
		}
		s.Require().NotNil(selected)
		s.Require().NotNil(other)
		s.Equal(float64(50), selected.DurationPerExecMs, "aggregate sums and counts before computing means")
		s.Equal(float64(21), selected.ExecCountPerSec)
		s.Equal(float64(70), selected.ScanRecordsPerSec)
		s.Equal(float64(70), exec, "auxiliary-only plans belong to Other")
		s.Equal(float64(250), rows)
		if top == 1 {
			s.InDelta(120.0/7, other.DurationPerExecMs, 1e-8)
			s.Equal(uint64(8), other.CPUTimeMs)
		}
	}
}
