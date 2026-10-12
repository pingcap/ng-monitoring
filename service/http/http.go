package http

import (
	"io"
	"net"
	"net/http"
	"os"
	"path"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect"
	conprofhttp "github.com/pingcap/ng-monitoring/component/conprof/http"
	"github.com/pingcap/ng-monitoring/component/topsql"
	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/ng-monitoring/database/docdb"
	"github.com/pingcap/ng-monitoring/utils/logutil"

	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

var (
	httpServer *http.Server = nil
)

func ServeHTTP(l *config.Log, listener net.Listener, docDB docdb.DocDB) {
	gin.SetMode(gin.ReleaseMode)
	ng := gin.New()

	var err error
	accessLog := io.Writer(os.Stdout)
	if l.Path != "" {
		logFileName := path.Join(l.Path, "service.log")
		rotateWriter, err := logutil.NewRotateWriter(l.FileLogConfig(logFileName))
		if err != nil {
			log.Fatal("Failed to open the log file", zap.String("filename", logFileName))
		}
		accessLog = rotateWriter
		defer func() {
			if err := rotateWriter.Close(); err != nil {
				log.Warn("failed to close http access log writer", zap.Error(err))
			}
		}()
	}
	ng.Use(gin.LoggerWithWriter(accessLog))

	// recovery
	ng.Use(gin.Recovery())

	ng.Handle(http.MethodGet, "/health", func(g *gin.Context) {
		g.JSON(http.StatusOK, Status{Health: true})
	})

	// route
	configGroup := ng.Group("/config")
	config.HTTPService(configGroup, docDB)
	topSQLGroup := ng.Group("/topsql")
	topsql.HTTPService(topSQLGroup)
	// register pprof http api
	pprof.Register(ng)

	continuousProfilingGroup := ng.Group("/continuous_profiling")
	conprofhttp.HTTPService(continuousProfilingGroup)

	promHandler := promhttp.Handler()
	promGroup := ng.Group("/metrics")
	promGroup.Any("", func(c *gin.Context) {
		promHandler.ServeHTTP(c.Writer, c.Request)
	})
	registerVictoriaMetricsRoutes(ng)
	httpServer = &http.Server{
		Handler:           ng,
		ReadHeaderTimeout: 5 * time.Second,
	}
	if err = httpServer.Serve(listener); err != nil && err != http.ErrServerClosed {
		log.Warn("failed to serve http service", zap.Error(err))
	}
}

type victoriaMetricsRequestHandler func(http.ResponseWriter, *http.Request) bool

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

// registerVictoriaMetricsRoutes exposes only the Prometheus-compatible API needed
// for remote write and Grafana queries. Keep the list explicit: falling back to
// VictoriaMetrics' handlers for unknown paths also exposes administrative APIs.
func registerVictoriaMetricsRoutes(router gin.IRoutes) {
	registerVictoriaMetricsRoutesWithHandlers(router, handleVictoriaMetricsInsert, handleVictoriaMetricsSelect)
}

func registerVictoriaMetricsRoutesWithHandlers(router gin.IRoutes, insertHandler, selectHandler gin.HandlerFunc) {
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
}

func handleVictoriaMetricsInsert(c *gin.Context) {
	handleVictoriaMetricsRequest(c, vminsert.RequestHandler)
}

func handleVictoriaMetricsSelect(c *gin.Context) {
	handleVictoriaMetricsRequest(c, vmselect.RequestHandler)
}

func handleVictoriaMetricsRequest(c *gin.Context, handler victoriaMetricsRequestHandler) {
	if !handler(c.Writer, c.Request) {
		c.String(http.StatusNotFound, "404 page not found")
	}
}

type Status struct {
	Health bool `json:"health"`
}

func StopHTTP() {
	if httpServer == nil {
		return
	}

	log.Info("shutting down http server")
	_ = httpServer.Close()
	log.Info("http server is down")
}
