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
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmstorage"
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
	httpServer    *http.Server = nil
	httpAccessLog io.Closer
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
		httpAccessLog = rotateWriter
	} else {
		httpAccessLog = nil
	}
	defer closeHTTPAccessLog()
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
	// compatible with victoria-metrics handlers
	ng.NoRoute(func(c *gin.Context) {
		handlerNoRouter(c)
	})
	httpServer = &http.Server{
		Handler:           ng,
		ReadHeaderTimeout: 5 * time.Second,
	}
	if err = httpServer.Serve(listener); err != nil && err != http.ErrServerClosed {
		log.Warn("failed to serve http service", zap.Error(err))
	}
}

// Try Victoria-Metrics' handlers first. If not handled, then return a 404 error.
func handlerNoRouter(c *gin.Context) {
	//reset to default
	c.Writer.WriteHeader(http.StatusOK)
	if vminsert.RequestHandler(c.Writer, c.Request) {
		return
	}

	if vmselect.RequestHandler(c.Writer, c.Request) {
		return
	}

	if vmstorage.RequestHandler(c.Writer, c.Request) {
		return
	}

	c.String(http.StatusNotFound, "404 page not found")
}

type Status struct {
	Health bool `json:"health"`
}

func StopHTTP() {
	if httpServer == nil {
		closeHTTPAccessLog()
		return
	}

	log.Info("shutting down http server")
	_ = httpServer.Close()
	closeHTTPAccessLog()
	log.Info("http server is down")
}

func closeHTTPAccessLog() {
	if httpAccessLog == nil {
		return
	}
	if err := httpAccessLog.Close(); err != nil {
		log.Warn("failed to close http access log writer", zap.Error(err))
	}
	httpAccessLog = nil
}
