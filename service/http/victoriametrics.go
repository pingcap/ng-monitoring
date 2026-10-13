package http

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmstorage"
	"github.com/gin-gonic/gin"
	"github.com/pingcap/log"
)

type victoriaMetricsRequestHandler func(http.ResponseWriter, *http.Request) bool

type VictoriaMetricsRoute struct {
	Method string
	Path   string
}

type VictoriaMetricsAllowlist struct {
	AllowAll bool
	Routes   []VictoriaMetricsRoute
}

type victoriaMetricsAllowlistFile struct {
	Routes []string `toml:"routes"`
}

var victoriaMetricsGetAndPostPaths = []string{
	"/api/v1/query",
	"/api/v1/query_range",
	"/api/v1/series",
	"/api/v1/labels",
	"/api/v1/query_exemplars",
}

var victoriaMetricsGetPaths = []string{
	"/api/v1/label/:name/values",
	"/api/v1/metadata",
	"/api/v1/rules",
	"/api/v1/alerts",
	"/api/v1/status/buildinfo",
	"/api/v1/status/tsdb",
	"/federate",
}

var supportedHTTPMethods = map[string]struct{}{
	http.MethodConnect: {},
	http.MethodDelete:  {},
	http.MethodGet:     {},
	http.MethodHead:    {},
	http.MethodOptions: {},
	http.MethodPatch:   {},
	http.MethodPost:    {},
	http.MethodPut:     {},
	http.MethodTrace:   {},
}

func LoadVictoriaMetricsAllowlist(fileName string) (VictoriaMetricsAllowlist, error) {
	if fileName == "" {
		return VictoriaMetricsAllowlist{}, nil
	}

	var file victoriaMetricsAllowlistFile
	metadata, err := toml.DecodeFile(fileName, &file)
	if err != nil {
		return VictoriaMetricsAllowlist{}, fmt.Errorf("decode %q: %w", fileName, err)
	}
	if undecoded := metadata.Undecoded(); len(undecoded) > 0 {
		return VictoriaMetricsAllowlist{}, fmt.Errorf("decode %q: unknown field %q", fileName, undecoded[0].String())
	}

	allowlist := VictoriaMetricsAllowlist{}
	seen := make(map[VictoriaMetricsRoute]struct{}, len(file.Routes))
	for i, entry := range file.Routes {
		entry = strings.TrimSpace(entry)
		if entry == "*" {
			allowlist.AllowAll = true
			continue
		}

		fields := strings.Fields(entry)
		if len(fields) != 2 {
			return VictoriaMetricsAllowlist{}, fmt.Errorf("invalid route %d %q: expected METHOD PATH or *", i, entry)
		}
		method := strings.ToUpper(fields[0])
		if _, ok := supportedHTTPMethods[method]; !ok {
			return VictoriaMetricsAllowlist{}, fmt.Errorf("invalid route %d %q: unsupported HTTP method %q", i, entry, fields[0])
		}
		routePath := fields[1]
		if !strings.HasPrefix(routePath, "/") || strings.ContainsAny(routePath, "?#") {
			return VictoriaMetricsAllowlist{}, fmt.Errorf("invalid route %d %q: path must be absolute and cannot contain a query or fragment", i, entry)
		}
		if strings.ContainsAny(routePath, ":*") {
			return VictoriaMetricsAllowlist{}, fmt.Errorf("invalid route %d %q: only exact paths are supported", i, entry)
		}

		route := VictoriaMetricsRoute{Method: method, Path: routePath}
		if _, ok := seen[route]; ok {
			continue
		}
		seen[route] = struct{}{}
		allowlist.Routes = append(allowlist.Routes, route)
	}

	return allowlist, nil
}

// registerVictoriaMetricsRoutes exposes the built-in Prometheus-compatible API
// plus exact routes from the optional local allowlist. Unknown paths remain 404
// unless the operator explicitly opts into the legacy unrestricted fallback.
func registerVictoriaMetricsRoutes(router *gin.Engine, allowlist VictoriaMetricsAllowlist) {
	registerVictoriaMetricsRoutesWithHandlers(
		router,
		allowlist,
		handleVictoriaMetricsInsert,
		handleVictoriaMetricsSelect,
		handleVictoriaMetricsFallback,
	)
}

func registerVictoriaMetricsRoutesWithHandlers(
	router *gin.Engine,
	allowlist VictoriaMetricsAllowlist,
	insertHandler, selectHandler, fallbackHandler gin.HandlerFunc,
) {
	for _, prefix := range []string{"", "/prometheus"} {
		router.Handle(http.MethodPost, prefix+"/api/v1/write", insertHandler)
		for _, routePath := range victoriaMetricsGetAndPostPaths {
			for _, method := range []string{http.MethodGet, http.MethodPost} {
				router.Handle(method, prefix+routePath, selectHandler)
			}
		}
		for _, routePath := range victoriaMetricsGetPaths {
			router.Handle(http.MethodGet, prefix+routePath, selectHandler)
		}
	}

	if len(allowlist.Routes) == 0 && !allowlist.AllowAll {
		return
	}

	if allowlist.AllowAll {
		log.Warn("VictoriaMetrics wildcard HTTP API allowlist is enabled; all APIs handled by embedded VictoriaMetrics components are exposed")
	}

	additionalRoutes := make(map[VictoriaMetricsRoute]struct{}, len(allowlist.Routes))
	for _, route := range allowlist.Routes {
		additionalRoutes[route] = struct{}{}
	}
	router.NoRoute(func(c *gin.Context) {
		route := VictoriaMetricsRoute{Method: c.Request.Method, Path: c.Request.URL.Path}
		if _, ok := additionalRoutes[route]; allowlist.AllowAll || ok {
			// Gin initializes NoRoute responses with a 404 status before invoking
			// this handler. Reset it so VictoriaMetrics can set the final status.
			c.Writer.WriteHeader(http.StatusOK)
			fallbackHandler(c)
			return
		}
		c.String(http.StatusNotFound, "404 page not found")
	})
}

func handleVictoriaMetricsInsert(c *gin.Context) {
	handleVictoriaMetricsRequest(c, vminsert.RequestHandler)
}

func handleVictoriaMetricsSelect(c *gin.Context) {
	handleVictoriaMetricsRequest(c, vmselect.RequestHandler)
}

func handleVictoriaMetricsFallback(c *gin.Context) {
	for _, handler := range []victoriaMetricsRequestHandler{
		vminsert.RequestHandler,
		vmselect.RequestHandler,
		vmstorage.RequestHandler,
	} {
		if handler(c.Writer, c.Request) {
			return
		}
	}
	c.String(http.StatusNotFound, "404 page not found")
}

func handleVictoriaMetricsRequest(c *gin.Context, handler victoriaMetricsRequestHandler) {
	if !handler(c.Writer, c.Request) {
		c.String(http.StatusNotFound, "404 page not found")
	}
}
