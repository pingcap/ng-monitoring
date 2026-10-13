package http

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestVictoriaMetricsAllowlist(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	registerVictoriaMetricsRoutesWithHandlers(router, VictoriaMetricsAllowlist{}, successHandler, successHandler, successHandler)

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

func TestRequireDashboardAuth(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	protected := router.Group("/protected")
	protected.Use(requireDashboardAuth("test-token"))
	protected.GET("", successHandler)
	router.GET("/public", successHandler)

	require.Equal(t, http.StatusUnauthorized, performRequest(router, http.MethodGet, "/protected").Code)

	request := httptest.NewRequest(http.MethodGet, "/protected", nil)
	request.Header.Set(DashboardAuthHeader, "wrong-token")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	require.Equal(t, http.StatusUnauthorized, response.Code)

	request = httptest.NewRequest(http.MethodGet, "/protected", nil)
	request.Header.Set(DashboardAuthHeader, "test-token")
	response = httptest.NewRecorder()
	router.ServeHTTP(response, request)
	require.Equal(t, http.StatusNoContent, response.Code)

	require.Equal(t, http.StatusNoContent, performRequest(router, http.MethodGet, "/public").Code)
}

func TestVictoriaMetricsRejectsUnlistedAPIs(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	registerVictoriaMetricsRoutesWithHandlers(router, VictoriaMetricsAllowlist{}, successHandler, successHandler, successHandler)

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

func TestVictoriaMetricsAdditionalAllowlist(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	registerVictoriaMetricsRoutesWithHandlers(router, VictoriaMetricsAllowlist{
		Routes: []VictoriaMetricsRoute{
			{Method: http.MethodGet, Path: "/api/v1/extra"},
		},
	}, successHandler, successHandler, writeOnlyHandler)

	require.Equal(t, http.StatusOK, performRequest(router, http.MethodGet, "/api/v1/extra").Code)
	require.Equal(t, http.StatusNotFound, performRequest(router, http.MethodPost, "/api/v1/extra").Code)
	require.Equal(t, http.StatusNotFound, performRequest(router, http.MethodGet, "/api/v1/other").Code)
}

func TestVictoriaMetricsWildcardAllowlist(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.GET("/health", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})
	registerVictoriaMetricsRoutesWithHandlers(router, VictoriaMetricsAllowlist{AllowAll: true}, successHandler, successHandler, successHandler)

	require.Equal(t, http.StatusNoContent, performRequest(router, http.MethodPost, "/api/v1/admin/tsdb/delete_series").Code)
	require.Equal(t, http.StatusNoContent, performRequest(router, http.MethodGet, "/unknown").Code)
	require.Equal(t, http.StatusOK, performRequest(router, http.MethodGet, "/health").Code)
}

func TestLoadVictoriaMetricsAllowlist(t *testing.T) {
	testDir := t.TempDir()
	fileName := filepath.Join(testDir, "allowlist.toml")
	require.NoError(t, os.WriteFile(fileName, []byte(`
routes = [
  "GET /api/v1/extra",
  "post /api/v1/other",
  "GET /api/v1/extra",
  "*",
]`), 0600))

	allowlist, err := LoadVictoriaMetricsAllowlist(fileName)
	require.NoError(t, err)
	require.True(t, allowlist.AllowAll)
	require.Equal(t, []VictoriaMetricsRoute{
		{Method: http.MethodGet, Path: "/api/v1/extra"},
		{Method: http.MethodPost, Path: "/api/v1/other"},
	}, allowlist.Routes)
}

func TestLoadVictoriaMetricsAllowlistRejectsInvalidRoutes(t *testing.T) {
	tests := []string{
		`route = ["GET /api/v1/unknown-field"]`,
		`routes = ["/api/v1/missing-method"]`,
		`routes = ["INVALID /api/v1/route"]`,
		`routes = ["GET relative/path"]`,
		`routes = ["GET /api/v1/route?query=value"]`,
		`routes = ["GET /api/v1/:route"]`,
	}

	for _, content := range tests {
		t.Run(content, func(t *testing.T) {
			fileName := filepath.Join(t.TempDir(), "allowlist.toml")
			require.NoError(t, os.WriteFile(fileName, []byte(content), 0600))
			_, err := LoadVictoriaMetricsAllowlist(fileName)
			require.Error(t, err)
		})
	}
}

func successHandler(c *gin.Context) {
	c.Status(http.StatusNoContent)
}

func writeOnlyHandler(c *gin.Context) {
	_, _ = c.Writer.Write([]byte("ok"))
}

func performRequest(handler http.Handler, method, requestPath string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, requestPath, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}
