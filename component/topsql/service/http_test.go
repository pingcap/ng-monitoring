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
	orderBy   string
	calls     int
	onSummary func(context.Context) error
}

func (s *summaryQueryStub) Records(string, int, int, int, int, string, string, *[]query.RecordItem) error {
	return nil
}

func (s *summaryQueryStub) Summary(ctx context.Context, _ int, _ int, _ int, _ int, _ string, _ string, _ *[]query.SummaryItem, orderBy string) error {
	s.orderBy = orderBy
	s.calls++
	if s.onSummary != nil {
		return s.onSummary(ctx)
	}
	return nil
}

func (s *summaryQueryStub) SummaryBy(_ int, _ int, _ int, _ int, _ string, _ string, _ string, _ *[]query.SummaryByItem, orderBy string) error {
	s.orderBy = orderBy
	s.calls++
	return nil
}

func (s *summaryQueryStub) Instances(int, int, *[]query.InstanceItem) error { return nil }
func (s *summaryQueryStub) Close()                                          {}

func TestSummaryHandlerOrderByCompatibility(t *testing.T) {
	gin.SetMode(gin.TestMode)

	testCases := []struct {
		name            string
		parameter       string
		expectedStatus  int
		expectedOrderBy string
		expectedCalls   int
	}{
		{"snake case parameter", "order_by=logical_read", http.StatusOK, query.OrderByLogicalRead, 1},
		{"legacy camel case parameter", "orderBy=block_read", http.StatusOK, query.OrderByBlockRead, 1},
		{"invalid value", "order_by=invalid", http.StatusBadRequest, "", 0},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			stub := &summaryQueryStub{}
			service := NewService(stub)
			router := gin.New()
			router.GET("/v1/summary", service.summaryHandler)

			request := httptest.NewRequest(http.MethodGet, "/v1/summary?instance=127.0.0.1%3A20160&instance_type=tikv&start=1&end=2&"+testCase.parameter, nil)
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)

			require.Equal(t, testCase.expectedStatus, response.Code)
			require.Equal(t, testCase.expectedCalls, stub.calls)
			require.Equal(t, testCase.expectedOrderBy, stub.orderBy)
		})
	}
}

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
