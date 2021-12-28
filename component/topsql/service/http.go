package service

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/ng-monitoring/component/topsql/query"
	"github.com/pingcap/ng-monitoring/component/topsql/store"
)

var (
	topSQLItemsP   = TopSQLItemsPool{}
	instanceItemsP = InstanceItemsPool{}

	metricNames = []string{
		store.MetricNameCPUTime,
		store.MetricNameReadRow,
		store.MetricNameReadIndex,
		store.MetricNameWriteRow,
		store.MetricNameWriteIndex,
		store.MetricNameSQLExecCount,
		store.MetricNameSQLDurationSum,
		store.VirtualMetricNameSQLDuration,
	}
)

type Service struct {
	query query.Query
}

func NewService(query query.Query) *Service {
	return &Service{query: query}
}

func (s *Service) HTTPService(g *gin.RouterGroup) {
	g.GET("/v1/instances", s.instancesHandler)
	for _, name := range metricNames {
		g.GET("/v1/"+name, s.metricHandler(name))
	}
}

func (s *Service) instancesHandler(c *gin.Context) {
	instances := instanceItemsP.Get()
	defer instanceItemsP.Put(instances)

	if err := s.query.AllInstances(instances); err != nil {
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

func (s *Service) metricHandler(name string) gin.HandlerFunc {
	return func(c *gin.Context) {
		s.queryMetric(c, name)
	}
}

func (s *Service) queryMetric(c *gin.Context, name string) {
	instance := c.Query("instance")
	if len(instance) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "error",
			"message": "no instance",
		})
		return
	}

	instanceType := c.Query("instance_type")
	if len(instanceType) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "error",
			"message": "no instance_type",
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

	err = s.query.TopSQL(name, int(startSecs), int(endSecs), int(windowSecs), int(top), instance, instanceType, items)
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
