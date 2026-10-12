package http

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestVictoriaMetricsAllowlist(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	registerVictoriaMetricsRoutesWithHandlers(router, successHandler, successHandler)

	allowed := []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/api/v1/write"},
		{http.MethodPost, "/prometheus/api/v1/write"},
		{http.MethodGet, "/api/v1/query"},
		{http.MethodPost, "/api/v1/query_range"},
		{http.MethodGet, "/api/v1/series"},
		{http.MethodPost, "/api/v1/labels"},
		{http.MethodGet, "/api/v1/label/instance/values"},
		{http.MethodGet, "/prometheus/api/v1/label/__name__/values"},
		{http.MethodGet, "/api/v1/metadata"},
		{http.MethodPost, "/api/v1/query_exemplars"},
		{http.MethodGet, "/api/v1/rules"},
		{http.MethodGet, "/api/v1/alerts"},
		{http.MethodGet, "/api/v1/status/buildinfo"},
		{http.MethodGet, "/api/v1/status/tsdb"},
		{http.MethodGet, "/federate"},
		{http.MethodGet, "/prometheus/federate"},
	}

	for _, tc := range allowed {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			response := performRequest(router, tc.method, tc.path)
			require.Equal(t, http.StatusNoContent, response.Code)
		})
	}
}

func TestVictoriaMetricsRejectsUnlistedAPIs(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	registerVictoriaMetricsRoutesWithHandlers(router, successHandler, successHandler)

	rejected := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/api/v1/write"},
		{http.MethodPost, "/api/v1/metadata"},
		{http.MethodPost, "/prometheus/federate"},
		{http.MethodPost, "/api/v1/import"},
		{http.MethodPost, "/api/v1/import/native"},
		{http.MethodPost, "/api/v1/admin/tsdb/delete_series"},
		{http.MethodPost, "/prometheus/api/v1/admin/tsdb/delete_series"},
		{http.MethodPost, "/api/v1/admin/tsdb/snapshot"},
		{http.MethodPost, "/internal/force_merge"},
		{http.MethodPost, "/internal/force_flush"},
		{http.MethodPost, "/internal/resetRollupResultCache"},
		{http.MethodGet, "/snapshot/list"},
		{http.MethodPost, "/-/reload"},
		{http.MethodGet, "/unknown"},
	}

	for _, tc := range rejected {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			response := performRequest(router, tc.method, tc.path)
			require.Equal(t, http.StatusNotFound, response.Code)
		})
	}
}

func successHandler(c *gin.Context) {
	c.Status(http.StatusNoContent)
}

func performRequest(handler http.Handler, method, requestPath string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, requestPath, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}
