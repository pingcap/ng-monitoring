package query

import (
	"context"
	"net/http"
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

func TestQueryRemainingBudget(t *testing.T) {
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
