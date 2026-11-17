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
	for _, top := range []int{5, 100, 101, 150, 200, 250} {
		s.Run(fmt.Sprintf("top=%d", top), func() {
			positive, negative := 0, 0
			dq := query.NewDefaultQuery(func(w http.ResponseWriter, r *http.Request) {
				expr := r.URL.Query().Get("query")
				s.Require().LessOrEqual(len(expr), 15*1024)
				for _, match := range matcher.FindAllStringSubmatch(expr, -1) {
					count := len(strings.Split(match[2], "|"))
					s.LessOrEqual(count, top)
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
			s.Require().NoError(dq.Summary(context.Background(), int(testBaseTs), int(testBaseTs+6), 6, top, instance, "tikv", &items))
			s.Require().Len(items, top+1)
			if top == 250 {
				s.Greater(positive, 1)
				s.Zero(negative)
			} else {
				s.Equal(1, positive)
				s.Equal(1, negative)
			}
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
