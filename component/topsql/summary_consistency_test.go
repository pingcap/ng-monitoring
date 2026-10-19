package topsql_test

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/pingcap/ng-monitoring/component/topsql/query"
	"github.com/pingcap/ng-monitoring/database/timeseries"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/regexutil"
)

func (s *testTopSQLSuite) TestSummaryWritesBetweenTopAndTotal() {
	const top = 300 // Forces total-minus-selected for both network and auxiliary queries.
	for _, target := range []string{"none", "selected", "unselected"} {
		s.Run(target, func() {
			instance := "between-top-total-" + target
			line := func(metric, digest, plan, table string, value int) string {
				return fmt.Sprintf("%s{instance=%q,instance_type=\"tikv\",sql_digest=%q,plan_digest=%q,table=%q} %d %d\n", metric, instance, digest, plan, table, value, testBaseTs*1000)
			}
			digest := func(i int) string { return fmt.Sprintf("%064x", i) }
			var initial strings.Builder
			for i := 1; i <= top+1; i++ {
				usage, exec := 2, 2
				if i == 1 {
					usage, exec = 1000, 100
				} else if i == top+1 {
					usage, exec = 1, 1
				}
				initial.WriteString(line("network_in_bytes", digest(i), "aa", "base", usage))
				initial.WriteString(line("network_out_bytes", digest(i), "aa", "base", 0))
				initial.WriteString(line("sql_exec_count", digest(i), "aa", "base", exec))
			}
			initial.WriteString(line("network_in_bytes", "", "", "", 9))
			initial.WriteString(line("network_out_bytes", "", "", "", 0))
			initial.WriteString(line("sql_exec_count", "", "", "", 9))
			s.importSummarySamples(initial.String())

			curveBatches, auxBatches := 0, 0
			curveTotals, auxTotals := 0, 0
			dq := query.NewDefaultQuery(func(w http.ResponseWriter, r *http.Request) {
				expr := r.URL.Query().Get("query")
				s.Require().LessOrEqual(len(expr), 15*1024)
				selectedFilter := strings.Contains(expr, "sql_digest=~")
				global := !strings.Contains(expr, "sql_digest")
				curve := r.URL.Path == "/api/v1/query_range"
				auxiliary := strings.Contains(expr, "sql_exec_count")
				if selectedFilter && curve {
					curveBatches++
				}
				if selectedFilter && auxiliary {
					auxBatches++
				}
				if global && (curve || auxiliary) {
					if curve {
						curveTotals++
						s.Require().Greater(curveBatches, 1)
					} else {
						auxTotals++
						s.Require().Greater(auxBatches, 1)
					}
					if target != "none" {
						i := 1
						if target == "unselected" {
							i = top + 1
						}
						if curve {
							s.importSummarySamples(line("network_in_bytes", digest(i), "bb", "late", 20) + line("network_out_bytes", digest(i), "bb", "late", 0))
						} else {
							s.importSummarySamples(line("sql_exec_count", digest(i), "aa", "late", 20))
						}
					}
				}
				timeseries.SelectHandler(w, r)
			}, s.docDB)
			var items []query.SummaryItem
			s.Require().NoError(dq.Summary(context.Background(), int(testBaseTs), int(testBaseTs), 6, top, instance, "tikv", &items, query.OrderByNetwork))
			s.Require().Len(items, top+1)
			s.Equal(1, curveTotals)
			s.Equal(1, auxTotals)
			var other *query.SummaryItem
			var usage uint64
			var executions float64
			for i := range items {
				usage += items[i].NetworkBytes
				executions += items[i].ExecCountPerSec
				if items[i].IsOther {
					other = &items[i]
				}
				if items[i].SQLDigest == digest(1) {
					s.Equal(uint64(1000), items[i].NetworkBytes)
					s.Equal(float64(100), items[i].ExecCountPerSec)
					s.Len(items[i].Plans, 1, "a plan arriving after the selected curves is not retroactively added")
				}
			}
			s.Require().NotNil(other)
			expectedOther, expectedTotal := uint64(10), uint64(1608)
			if target != "none" {
				expectedOther += 20
				expectedTotal += 20
			}
			s.Equal(expectedOther, other.NetworkBytes)
			s.Equal(float64(expectedOther), other.ExecCountPerSec)
			s.Equal(expectedTotal, usage)
			s.Equal(float64(expectedTotal-900), executions)
			// Conservation alone does not imply correct attribution across reads.
			trueOther := uint64(10)
			if target == "unselected" {
				trueOther += 20
			}
			if target == "selected" {
				s.Equal(uint64(20), other.NetworkBytes-trueOther)
			} else {
				s.Equal(trueOther, other.NetworkBytes)
			}
			s.T().Logf("target=%s selected_batches=%d aux_batches=%d other=%d true_other=%d total=%d", target, curveBatches, auxBatches, other.NetworkBytes, trueOther, usage)
		})
	}
}

func (s *testTopSQLSuite) TestSummaryLargeCPURegexp() {
	const sqlCount = 300
	instance := "large-cpu-regexp"
	var lines strings.Builder
	for i := 1; i <= sqlCount; i++ {
		for plan := 1; plan <= 2; plan++ {
			for table := 1; table <= 2; table++ {
				for offset := 0; offset <= 6; offset += 6 {
					labels := fmt.Sprintf("instance=%q,instance_type=\"tikv\",sql_digest=\"%064x\",plan_digest=\"%02x\",table=\"t%d\"", instance, i, plan, table)
					fmt.Fprintf(&lines, "cpu_time{%s} %d %d\nsql_exec_count{%s} 1 %d\n", labels, i*plan*table, (testBaseTs+uint64(offset))*1000, labels, (testBaseTs+uint64(offset))*1000)
				}
			}
		}
	}
	fmt.Fprintf(&lines, "cpu_time{instance=%q,instance_type=\"tikv\"} 7 %d\nsql_exec_count{instance=%q,instance_type=\"tikv\"} 25 %d\n", instance, testBaseTs*1000, instance, testBaseTs*1000)
	s.importSummarySamples(lines.String())
	matcher := regexp.MustCompile(`sql_digest(=~|!~)"([^"]+)"`)
	for _, top := range []int{100, 101, 150, 200} {
		s.Run(fmt.Sprintf("top=%d", top), func() {
			positive, negative := 0, 0
			dq := query.NewDefaultQuery(func(w http.ResponseWriter, r *http.Request) {
				expr := r.URL.Query().Get("query")
				s.Require().LessOrEqual(len(expr), 15*1024)
				for _, match := range matcher.FindAllStringSubmatch(expr, -1) {
					count := len(strings.Split(match[2], "|"))
					s.Equal(top, count, "CPU filters fit in a single expression in these cases")
					if count > 100 {
						s.Empty(regexutil.GetOrValuesPromRegex(match[2]), "literal extraction has exceeded VM's 100-value limit")
					} else {
						s.Len(regexutil.GetOrValuesPromRegex(match[2]), count)
					}
					if strings.Contains(expr, "cpu_time") {
						if match[1] == "=~" {
							positive++
						} else {
							negative++
						}
					}
				}
				timeseries.SelectHandler(w, r)
			}, s.docDB)
			var items []query.SummaryItem
			s.Require().NoError(dq.Summary(context.Background(), int(testBaseTs), int(testBaseTs+6), 6, top, instance, "tikv", &items, query.OrderByCPU))
			s.Require().Len(items, top+1)
			s.Equal(1, positive)
			s.Equal(1, negative)
			var usage uint64
			var exec float64
			selected := 0
			for _, item := range items {
				usage += item.CPUTimeMs
				exec += item.ExecCountPerSec
				if item.IsOther {
					remaining := sqlCount - top
					s.Equal(uint64(18*remaining*(remaining+1)/2+7), item.CPUTimeMs)
					continue
				}
				selected++
				var id int
				_, err := fmt.Sscanf(item.SQLDigest, "%x", &id)
				s.NoError(err)
				s.Greater(id, sqlCount-top)
				s.Equal(uint64(18*id), item.CPUTimeMs)
				s.Len(item.Plans, 2)
				for _, plan := range item.Plans {
					s.Len(plan.TimestampSec, 2)
				}
			}
			s.Equal(top, selected)
			s.Equal(uint64(18*sqlCount*(sqlCount+1)/2+7), usage)
			s.InDelta(float64(8*sqlCount+25)/7, exec, 1e-8)
			s.T().Logf("top=%d positive_candidates=%d negative_candidates=%d literal_fast_path_eligible=%t total=%d", top, top, top, top <= 100, usage)
		})
	}
}
