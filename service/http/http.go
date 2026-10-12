package http

import (
	"crypto/subtle"
	"io"
	"net"
	"net/http"
	"os"
	"path"
	"time"

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

const DashboardAuthHeader = "X-Ng-Monitoring-Dashboard-Token"

func ServeHTTP(l *config.Log, listener net.Listener, docDB docdb.DocDB, vmAllowlist VictoriaMetricsAllowlist, dashboardAuthToken string) {
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

	// Dashboard-only and diagnostic APIs contain configuration, SQL text, query
	// plans, and profiles. Keep them behind a credential that is advertised to
	// Dashboard through the existing PD topology entry.
	dashboardOnly := ng.Group("")
	dashboardOnly.Use(requireDashboardAuth(dashboardAuthToken))
	configGroup := dashboardOnly.Group("/config")
	config.HTTPService(configGroup, docDB)
	topSQLGroup := dashboardOnly.Group("/topsql")
	topsql.HTTPService(topSQLGroup)
	pprof.RouteRegister(dashboardOnly)

	continuousProfilingGroup := dashboardOnly.Group("/continuous_profiling")
	conprofhttp.HTTPService(continuousProfilingGroup)

	promHandler := promhttp.Handler()
	promGroup := ng.Group("/metrics")
	promGroup.Any("", func(c *gin.Context) {
		promHandler.ServeHTTP(c.Writer, c.Request)
	})
	registerVictoriaMetricsRoutes(ng, vmAllowlist)
	httpServer = &http.Server{
		Handler:           ng,
		ReadHeaderTimeout: 5 * time.Second,
	}
	if err = httpServer.Serve(listener); err != nil && err != http.ErrServerClosed {
		log.Warn("failed to serve http service", zap.Error(err))
	}
}

func requireDashboardAuth(expectedToken string) gin.HandlerFunc {
	return func(c *gin.Context) {
		providedToken := c.GetHeader(DashboardAuthHeader)
		if expectedToken == "" || len(providedToken) != len(expectedToken) ||
			subtle.ConstantTimeCompare([]byte(providedToken), []byte(expectedToken)) != 1 {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"status":  "error",
				"message": "dashboard authentication required",
			})
			return
		}
		c.Next()
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
