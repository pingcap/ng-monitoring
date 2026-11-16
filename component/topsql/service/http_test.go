package service

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/pingcap/ng-monitoring/component/topsql/query"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type summaryQueryStub struct {
	calls     int
	onSummary func(context.Context) error
}

func (s *summaryQueryStub) Records(string, int, int, int, int, string, string, *[]query.RecordItem) error {
	return nil
}

func (s *summaryQueryStub) Summary(ctx context.Context, _ int, _ int, _ int, _ int, _ string, _ string, _ *[]query.SummaryItem) error {
	s.calls++
	if s.onSummary != nil {
		return s.onSummary(ctx)
	}
	return nil
}

func (s *summaryQueryStub) SummaryBy(_ int, _ int, _ int, _ int, _ string, _ string, _ string, _ *[]query.SummaryByItem) error {
	s.calls++
	return nil
}

func (s *summaryQueryStub) Instances(int, int, *[]query.InstanceItem) error { return nil }
func (s *summaryQueryStub) Close()                                          {}

func TestSummaryHandlerCallerDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	deadline, _ := ctx.Deadline()
	stub := &summaryQueryStub{onSummary: func(got context.Context) error {
		gotDeadline, ok := got.Deadline()
		require.True(t, ok)
		require.Equal(t, deadline, gotDeadline)
		<-got.Done()
		return got.Err()
	}}
	router := gin.New()
	router.GET("/v1/summary", NewService(stub).summaryHandler)
	request := httptest.NewRequest(http.MethodGet, "/v1/summary?instance=test&instance_type=tikv&start=1&end=2", nil).WithContext(ctx)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	require.Equal(t, http.StatusServiceUnavailable, response.Code)
	require.Contains(t, response.Body.String(), context.DeadlineExceeded.Error())
	require.Equal(t, 1, stub.calls)
}

func TestSummaryHandlerPreservesCallerContext(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		callerDeadline, abort bool
	}{
		{"no deadline", false, false},
		{"caller deadline", true, false},
		{"caller cancel", false, true},
		{"caller deadline and cancel", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			if tc.callerDeadline {
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), time.Minute)
			}
			defer cancel()
			stub := &summaryQueryStub{onSummary: func(got context.Context) error {
				require.Equal(t, ctx, got, "the request context must pass through unchanged")
				deadline, ok := got.Deadline()
				wantDeadline, wantOK := ctx.Deadline()
				require.Equal(t, wantOK, ok)
				require.Equal(t, wantDeadline, deadline)
				if tc.abort {
					cancel()
					require.ErrorIs(t, got.Err(), context.Canceled)
					return got.Err()
				}
				return nil
			}}
			router := gin.New()
			router.GET("/v1/summary", NewService(stub).summaryHandler)
			r := httptest.NewRequest(http.MethodGet, "/v1/summary?instance=test&instance_type=tikv&start=1&end=2", nil).WithContext(ctx)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, r)
			if tc.abort {
				require.Equal(t, http.StatusServiceUnavailable, w.Code)
				require.Contains(t, w.Body.String(), context.Canceled.Error())
			} else {
				require.Equal(t, http.StatusOK, w.Code)
			}
			require.Equal(t, 1, stub.calls)
		})
	}
}
