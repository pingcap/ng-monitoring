package topsql_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/ng-monitoring/component/topsql/query"
	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/ng-monitoring/database"
	"github.com/pingcap/ng-monitoring/database/docdb"
	"github.com/pingcap/ng-monitoring/database/timeseries"
	"github.com/pingcap/ng-monitoring/utils"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmstorage"
)

// summaryBenchmarkMeta isolates VM/query processing from metadata storage IO.
// This benchmark is not a deployment or proxy-chain acceptance test.
type summaryBenchmarkMeta struct{ docdb.DocDB }

func (summaryBenchmarkMeta) QuerySQLMeta(ctx context.Context, _ string) (string, error) {
	return "", ctx.Err()
}

func (summaryBenchmarkMeta) QueryPlanMeta(ctx context.Context, _ string) (string, string, error) {
	return "", "", ctx.Err()
}

func BenchmarkSummaryFanout(b *testing.B) {
	cfg := config.GetDefaultConfig()
	cfg.Storage.Path = b.TempDir()
	database.Init(&cfg)
	defer database.Stop()

	const sqlCount, points = 300, 20
	base := int(time.Now().Unix()) - points*6
	var samples strings.Builder
	for sql := 1; sql <= sqlCount; sql++ {
		for plan := 1; plan <= 2; plan++ {
			for table := 1; table <= 2; table++ {
				labels := fmt.Sprintf("instance=\"benchmark\",instance_type=\"tikv\",sql_digest=\"%064x\",plan_digest=\"%02x\",table=\"t%d\"", sql, plan, table)
				for point := 0; point < points; point++ {
					usage := sql * plan * table
					for metric, value := range map[string]int{
						"cpu_time": usage, "network_in_bytes": usage, "network_out_bytes": 1,
						"logical_read_bytes": usage, "logical_write_bytes": 1,
						"sql_duration_sum": 1000000, "sql_duration_count": 1,
						"sql_exec_count": 1, "read_row": 1, "read_index": 1,
					} {
						fmt.Fprintf(&samples, "%s{%s} %d %d\n", metric, labels, value, (base+point*6)*1000)
					}
				}
			}
		}
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/import/prometheus", strings.NewReader(samples.String()))
	response := httptest.NewRecorder()
	timeseries.InsertHandler(response, req)
	if response.Code != http.StatusNoContent {
		b.Fatal(response.Body.String())
	}
	vmstorage.Storage.DebugFlush()

	var returnedBytes int64
	dq := query.NewDefaultQuery(func(w http.ResponseWriter, r *http.Request) {
		timeseries.SelectHandler(w, r)
		// Read the existing VM buffer size without copying or re-decoding it.
		returnedBytes += int64(w.(*utils.ResponseWriter).Body.Len())
	}, summaryBenchmarkMeta{})
	for _, orderBy := range []string{query.OrderByCPU, query.OrderByNetwork, query.OrderByLogicalIO} {
		b.Run(orderBy, func(b *testing.B) {
			for _, top := range []int{5, 100, 101, 150, 200} {
				b.Run(fmt.Sprintf("top%d", top), func(b *testing.B) {
					returnedBytes = 0
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						var items []query.SummaryItem
						if err := dq.Summary(context.Background(), base, base+(points-1)*6, 6, top, "benchmark", "tikv", &items, orderBy); err != nil {
							b.Fatal(err)
						}
					}
					b.StopTimer()
					b.ReportMetric(float64(returnedBytes)/float64(b.N), "VM-JSON-B/op")
				})
			}
		})
	}
}
