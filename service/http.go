package service

import (
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/zhongzc/diag_backend/storage/query"

	"github.com/gin-contrib/cors"
	"github.com/gin-contrib/gzip"
	"github.com/gin-gonic/gin"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

var (
	httpServer *http.Server = nil

	topSQLItemsP   = TopSQLItemsPool{}
	instanceItemsP = InstanceItemsPool{}
)

func ServeHTTP(logFileName string, listener net.Listener) {
	gin.SetMode(gin.ReleaseMode)
	ng := gin.New()
	ng.Use(gin.Logger(), gin.Recovery())

	file, err := os.OpenFile(logFileName, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Fatal("Failed to open the log file", zap.String("filename", logFileName))
	}
	ng.Use(gin.LoggerWithWriter(file))

	// recovery
	ng.Use(gin.Recovery())

	// cors
	config := cors.DefaultConfig()
	config.AllowAllOrigins = true
	ng.Use(cors.New(config))

	// gzip
	ng.Use(gzip.Gzip(gzip.DefaultCompression))

	// route
	ng.GET("/topsql/v1/cpu_time", topSQLCPUTime)
	ng.GET("/topsql/v1/instances", topSQLAllInstances)

	httpServer = &http.Server{Handler: ng}
	if err = httpServer.Serve(listener); err != nil && err != http.ErrServerClosed {
		log.Warn("failed to serve http service", zap.Error(err))
	}
}

func StopHTTP() {
	if httpServer == nil {
		return
	}

	log.Info("shutting down http server")
	_ = httpServer.Close()
}

func topSQLCPUTime(c *gin.Context) {
	instance := c.Query("instance")
	if len(instance) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "error",
			"message": "no instance",
		})
		return
	}

	var err error
	now := time.Now().Unix()

	var startSecs float64
	var endSecs float64
	var top int64
	var windowSecs int64

	const weekSecs = 7 * 24 * 60 * 60
	defaultStart := strconv.Itoa(int(now - 2*weekSecs))
	defaultEnd := strconv.Itoa(int(now))
	defaultTop := "-1"
	defaultWindow := "1m"

	raw := c.DefaultQuery("start", defaultStart)
	if len(raw) == 0 {
		raw = defaultStart
	}
	startSecs, err = strconv.ParseFloat(raw, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "error",
			"message": err.Error(),
		})
		return
	}

	raw = c.DefaultQuery("end", strconv.Itoa(int(now)))
	if len(raw) == 0 {
		raw = defaultEnd
	}
	endSecs, err = strconv.ParseFloat(raw, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "error",
			"message": err.Error(),
		})
		return
	}

	raw = c.DefaultQuery("top", "-1")
	if len(raw) == 0 {
		raw = defaultTop
	}
	top, err = strconv.ParseInt(raw, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "error",
			"message": err.Error(),
		})
		return
	}

	raw = c.DefaultQuery("window", "1m")
	if len(raw) == 0 {
		raw = defaultWindow
	}
	duration, err := time.ParseDuration(raw)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "error",
			"message": err.Error(),
		})
		return
	}
	windowSecs = int64(duration.Seconds())

	items := topSQLItemsP.Get()
	defer topSQLItemsP.Put(items)

	err = query.TopSQL(int(startSecs), int(endSecs), int(windowSecs), int(top), instance, items)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status":  "error",
			"message": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status": "ok",
		"data":   items,
	})
}

func topSQLAllInstances(c *gin.Context) {
	instances := instanceItemsP.Get()
	defer instanceItemsP.Put(instances)

	if err := query.AllInstances(instances); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status":  "error",
			"message": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status": "ok",
		"data":   instances,
	})
}
