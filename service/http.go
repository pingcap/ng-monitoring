package service

import (
	"net/http"
	"strconv"
	"time"

	"github.com/zhongzc/diag_backend/storage/query"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmstorage"
	"github.com/gin-contrib/cors"
	"github.com/gin-contrib/gzip"
	"github.com/gin-gonic/gin"
)

var (
	ginEngine = gin.Default()

	cpuTimeSliceP  = CPUTimeSlicePool{}
	topSQLItemsP   = TopSQLItemsPool{}
	instanceItemsP = InstanceItemsPool{}
)

func init() {
	config := cors.DefaultConfig()
	config.AllowAllOrigins = true

	ginEngine.Use(cors.New(config))
	ginEngine.Use(gzip.Gzip(gzip.DefaultCompression))
	ginEngine.GET("/topsql/v1/cpu_time", topSQLCPUTime)
	ginEngine.GET("/topsql/v1/instances", topSQLAllInstances)

}

func requestHandler(w http.ResponseWriter, r *http.Request) bool {
	if vminsert.RequestHandler(w, r) {
		return true
	}
	if vmselect.RequestHandler(w, r) {
		return true
	}
	if vmstorage.RequestHandler(w, r) {
		return true
	}

	ginEngine.ServeHTTP(w, r)
	return true
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

	defaultStart := strconv.Itoa(int(now - 6*60*60))
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
