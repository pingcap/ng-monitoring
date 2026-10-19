package topsql_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/pingcap/ng-monitoring/component/topsql/query"
	"github.com/pingcap/ng-monitoring/database/timeseries"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmstorage"
)

func (s *testTopSQLSuite) importSummarySamples(lines string) {
	req := httptest.NewRequest(http.MethodPost, "/api/v1/import/prometheus", strings.NewReader(lines))
	w := httptest.NewRecorder()
	timeseries.InsertHandler(w, req)
	s.Require().Equal(http.StatusNoContent, w.Code, w.Body.String())
	vmstorage.Storage.DebugFlush()
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
