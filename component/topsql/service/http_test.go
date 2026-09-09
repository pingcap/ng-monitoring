package service

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pingcap/ng-monitoring/component/topsql/query"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type summaryQueryStub struct {
	orderBy string
	calls   int
}

func (s *summaryQueryStub) Records(string, int, int, int, int, string, string, *[]query.RecordItem) error {
	return nil
}

func (s *summaryQueryStub) Summary(_ int, _ int, _ int, _ int, _ string, _ string, _ *[]query.SummaryItem, orderBy string) error {
	s.orderBy = orderBy
	s.calls++
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
