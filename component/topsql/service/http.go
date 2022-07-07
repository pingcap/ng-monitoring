package service

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/pingcap/ng-monitoring/component/topsql/query"
	"github.com/pingcap/ng-monitoring/component/topsql/store"

	"github.com/gin-gonic/gin"
)

var (
	recordsP       = recordsPool{}
	summaryP       = summaryPool{}
	instanceItemsP = InstanceItemsPool{}

	metricNames = []string{
		store.MetricNameCPUTime,
		store.MetricNameReadRow,
		store.MetricNameReadIndex,
		store.MetricNameWriteRow,
		store.MetricNameWriteIndex,
		store.MetricNameSQLExecCount,
		store.MetricNameSQLDurationSum,
		store.MetricNameSQLDurationCount,
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
	g.GET("/v1/summary", s.summaryHandler)
}

func (s *Service) instancesHandler(c *gin.Context) {
	instances := instanceItemsP.Get()
	defer instanceItemsP.Put(instances)

	start, end, err := parseStartEnd(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "error",
			"message": err.Error(),
		})
		return
	}

	if err = s.query.Instances(start, end, instances); err != nil {
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

func (s *Service) summaryHandler(c *gin.Context) {
	start, end, windowSecs, top, instance, instanceType, err := parseAllParams(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "error",
			"message": err.Error(),
		})
		return
	}

	items := summaryP.Get()
	defer summaryP.Put(items)
	err = s.query.Summary(start, end, windowSecs, top, instance, instanceType, items)
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

func (s *Service) queryMetric(c *gin.Context, name string) {
	start, end, windowSecs, top, instance, instanceType, err := parseAllParams(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "error",
			"message": err.Error(),
		})
		return
	}

	items := recordsP.Get()
	defer recordsP.Put(items)
	err = s.query.Records(name, start, end, windowSecs, top, instance, instanceType, items)
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

func parseAllParams(c *gin.Context) (start, end, windowSecs, top int, instance, instanceType string, err error) {
	instance = c.Query("instance")
	if len(instance) == 0 {
		err = errors.New("no instance")
		return
	}

	instanceType = c.Query("instance_type")
	if len(instanceType) == 0 {
		err = errors.New("no instance_type")
		return
	}

	start, end, err = parseStartEnd(c)
	if err != nil {
		return
	}

	defaultTop := "-1"
	defaultWindow := "1m"
	raw := c.DefaultQuery("top", "-1")
	if len(raw) == 0 {
		raw = defaultTop
	}
	topInt64, err1 := strconv.ParseInt(raw, 10, 64)
	if err1 != nil {
		err = err1
		return
	}
	top = int(topInt64)

	raw = c.DefaultQuery("window", "1m")
	if len(raw) == 0 {
		raw = defaultWindow
	}
	duration, err1 := time.ParseDuration(raw)
	if err1 != nil {
		err = err1
		return
	}
	windowSecs = int(duration.Seconds())

	return
}

func parseStartEnd(c *gin.Context) (start, end int, err error) {
	now := time.Now().Unix()

	var startSecs float64
	var endSecs float64

	const weekSecs = 7 * 24 * 60 * 60
	defaultStart := strconv.Itoa(int(now - 2*weekSecs))
	defaultEnd := strconv.Itoa(int(now))

	raw := c.DefaultQuery("start", defaultStart)
	if len(raw) == 0 {
		raw = defaultStart
	}
	startSecs, err = strconv.ParseFloat(raw, 64)
	if err != nil {
		return
	}

	raw = c.DefaultQuery("end", strconv.Itoa(int(now)))
	if len(raw) == 0 {
		raw = defaultEnd
	}
	endSecs, err = strconv.ParseFloat(raw, 64)
	if err != nil {
		return
	}

	return int(startSecs), int(endSecs), nil
}
