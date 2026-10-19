package query

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/ng-monitoring/database/docdb"

	"github.com/stretchr/testify/require"
)

func TestQueryCancellation(t *testing.T) {
	for _, instant := range []bool{false, true} {
		name := "range"
		if instant {
			name = "instant"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			dq := NewDefaultQuery(func(w http.ResponseWriter, r *http.Request) {
				calls++
				require.Equal(t, ctx, r.Context())
				require.Empty(t, r.URL.Query().Get("timeout"), "a request without a caller deadline must not override VM's timeout")
				cancel()
				// If decoding runs after cancellation this would return a JSON error.
				_, _ = w.Write([]byte("invalid JSON"))
			}, nil)
			var response recordsMetricResp
			fetch := func() error {
				if instant {
					return dq.fetchInstantQuery(ctx, "cpu_time", 10, 6, &response)
				}
				return dq.fetchQueryRange(ctx, "cpu_time", 1, 10, 6, &response)
			}
			require.ErrorIs(t, fetch(), context.Canceled)
			require.Equal(t, 1, calls)
			require.ErrorIs(t, fetch(), context.Canceled)
			require.Equal(t, 1, calls, "a canceled request must not start another query")
		})
	}
}

func TestQueryRemainingCallerDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	deadline, _ := ctx.Deadline()
	var timeouts []time.Duration
	dq := NewDefaultQuery(func(w http.ResponseWriter, r *http.Request) {
		gotDeadline, ok := r.Context().Deadline()
		require.True(t, ok)
		require.Equal(t, deadline, gotDeadline)
		timeout, err := time.ParseDuration(r.URL.Query().Get("timeout"))
		require.NoError(t, err)
		require.Positive(t, timeout)
		require.LessOrEqual(t, timeout, 5*time.Second)
		timeouts = append(timeouts, timeout)
		// Ensure the second request has a measurably smaller remaining budget.
		time.Sleep(10 * time.Millisecond)
		_, _ = w.Write([]byte(`{"status":"success","data":{"result":[]}}`))
	}, nil)
	var response recordsMetricResp
	require.NoError(t, dq.fetchInstantQuery(ctx, "cpu_time", 10, 6, &response))
	require.NoError(t, dq.fetchQueryRange(ctx, "cpu_time", 1, 10, 6, &response))
	require.Less(t, timeouts[1], timeouts[0], "each phase must share the original deadline")
}

func TestSummaryStopsAfterQueuedQuery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	calls := 0
	dq := NewDefaultQuery(func(w http.ResponseWriter, r *http.Request) {
		calls++
		<-r.Context().Done()
		w.WriteHeader(http.StatusServiceUnavailable)
	}, nil)
	var items []SummaryItem
	require.ErrorIs(t, dq.Summary(ctx, 1, 10, 6, 5, "instance", "tikv", &items, OrderByCPU), context.DeadlineExceeded)
	require.Equal(t, 1, calls, "ranking timeout must prevent timeline and auxiliary queries")
	require.Empty(t, items)
}

type cancelMetadataDB struct {
	docdb.DocDB
	cancel context.CancelFunc
	plans  int
}

func (db *cancelMetadataDB) QuerySQLMeta(ctx context.Context, _ string) (string, error) {
	db.cancel()
	return "select 1", nil
}

func (db *cancelMetadataDB) QueryPlanMeta(context.Context, string) (string, string, error) {
	db.plans++
	return "", "", nil
}

func TestMetadataCancellationStopsPlanReads(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db := &cancelMetadataDB{cancel: cancel}
	dq := NewDefaultQuery(nil, db)
	groups := []sqlGroup{{sqlDigest: "sql", planSeries: []planSeries{{planDigest: "plan"}}}}
	filled := false
	require.ErrorIs(t, dq.fillText(ctx, "cpu_time", &groups, func(RecordItem) { filled = true }), context.Canceled)
	require.Zero(t, db.plans)
	require.False(t, filled)
}

func TestNegativeOtherDifferenceFailsQuery(t *testing.T) {
	// Simulate inconsistent reads, not ordinary append-only ingestion: the later
	// total is smaller than an already fetched selected value. A successful
	// response must not silently discard the resulting negative curve point or
	// return a negative auxiliary rate.
	digests := make([]string, 300)
	for i := range digests {
		digests[i] = digestLabel(i)
	}
	for _, auxiliary := range []bool{false, true} {
		t.Run(fmt.Sprintf("auxiliary=%t", auxiliary), func(t *testing.T) {
			selectedReads, totalReads := 0, 0
			dq := NewDefaultQuery(func(w http.ResponseWriter, r *http.Request) {
				expr := r.URL.Query().Get("query")
				require.LessOrEqual(t, len(expr), maxTSDBQueryBytes)
				require.NotContains(t, expr, "sql_digest!~", "the oversized Other filter must use the fallback")
				if strings.HasPrefix(expr, "topk(") {
					var ranking strings.Builder
					ranking.WriteString(`{"status":"success","data":{"result":[`)
					for i, digest := range digests {
						if i > 0 {
							ranking.WriteByte(',')
						}
						fmt.Fprintf(&ranking, `{"metric":{"sql_digest":%q},"value":[100,"1"]}`, digest)
					}
					ranking.WriteString(`]}}`)
					_, _ = w.Write([]byte(ranking.String()))
					return
				}
				result := `{"status":"success","data":{"result":[]}}`
				if strings.Contains(expr, "sql_digest=~") {
					selectedReads++
					if strings.Contains(expr, digests[0]) {
						if auxiliary {
							result = fmt.Sprintf(`{"status":"success","data":{"result":[{"metric":{"sql_digest":%q,"plan_digest":"plan"},"value":[100,"10"]}]}}`, digests[0])
						} else {
							result = fmt.Sprintf(`{"status":"success","data":{"result":[{"metric":{"sql_digest":%q,"plan_digest":"plan"},"values":[[100,"10"]]}]}}`, digests[0])
						}
					}
				} else {
					totalReads++
					if auxiliary {
						result = `{"status":"success","data":{"result":[{"metric":{},"value":[100,"9"]}]}}`
					} else {
						result = `{"status":"success","data":{"result":[{"metric":{},"values":[[100,"9"]]}]}}`
					}
				}
				_, _ = w.Write([]byte(result))
			}, nil)
			if auxiliary {
				values := map[RecordKey]float64{}
				err := dq.fetchSumFromTSDB(context.Background(), "sql_exec_count", 100, 100, "instance", "tikv", digests, true, values)
				require.ErrorContains(t, err, "total is smaller than selected")
				_, hasOther := values[RecordKey{}]
				require.False(t, hasOther)
			} else {
				var items []SummaryItem
				err := dq.Summary(context.Background(), 100, 100, 6, len(digests), "instance", "tikv", &items, OrderByNetwork)
				require.ErrorContains(t, err, "total is smaller than selected")
				require.Empty(t, items, "do not return a successful response with a silently dropped negative point")
			}
			require.Greater(t, selectedReads, 1)
			require.Equal(t, 1, totalReads)
		})
	}
}
