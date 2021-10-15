package continuousprofiling

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/log"
	"github.com/zhongzc/ng_monitoring/component/continuousprofiling/meta"
	"github.com/zhongzc/ng_monitoring/config"
	"go.uber.org/zap"
)

func HTTPService(g *gin.RouterGroup) {
	g.POST("/list", handleQueryList)
	g.POST("/download", handleDownload)
	g.GET("/components", handleComponents)
	g.GET("/estimate-size", handleEstimateSize)
}

func handleQueryList(c *gin.Context) {
	result, err := queryList(c)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status":  "error",
			"message": err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, result)
}

func handleDownload(c *gin.Context) {
	err := queryAndDownload(c)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status":  "error",
			"message": err.Error(),
		})
		return
	}
}

func handleComponents(c *gin.Context) {
	components := manager.GetCurrentScrapeComponents()
	c.JSON(http.StatusOK, components)
}

func handleEstimateSize(c *gin.Context) {
	days := 0
	if value := c.Request.FormValue("days"); len(value) > 0 {
		v, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"status":  "error",
				"message": "params days value is invalid, should be int",
			})
			return
		}
		days = int(v)
	}
	if days == 0 {
		c.JSON(http.StatusOK, 0)
		return
	}
	_, suites := manager.GetAllCurrentScrapeSuite()
	totalSize := 0
	for _, suite := range suites {
		size := suite.LastScrapeSize()
		if size == 0 {
			size = 500 * 1024
		}
		totalSize += size
	}
	cfg := config.GetGlobalConfig().ContinueProfiling
	compressRatio := 10
	estimateSize := (days * 24 * 60 * 60 / cfg.IntervalSeconds) * totalSize / compressRatio
	c.JSON(http.StatusOK, estimateSize)
}

func queryList(c *gin.Context) ([]meta.ProfileList, error) {
	param, err := getQueryParamFromBody(c.Request)
	if err != nil {
		return nil, err
	}

	return storage.QueryProfileList(param)
}

func queryAndDownload(c *gin.Context) error {
	param, err := getQueryParamFromBody(c.Request)
	if err != nil {
		return err
	}

	c.Writer.Header().
		Set("Content-Disposition",
			fmt.Sprintf(`attachment; filename="profile"`+time.Now().Format("20060102150405")+".zip"))
	zw := zip.NewWriter(c.Writer)
	fn := func(pt meta.ProfileTarget, ts int64, data []byte) error {
		fileName := fmt.Sprintf("%v_%v_%v_%v", pt.Kind, pt.Component, pt.Address, ts)
		fw, err := zw.Create(fileName)
		if err != nil {
			return err
		}
		_, err = fw.Write(data)
		return err
	}

	err = storage.QueryProfileData(param, fn)
	if err != nil {
		return err
	}
	err = zw.Close()
	if err != nil {
		log.Error("handle download request failed", zap.Error(err))
	}
	return nil
}

func getQueryParamFromBody(r *http.Request) (*meta.BasicQueryParam, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	if len(body) == 0 {
		return nil, nil
	}
	param := &meta.BasicQueryParam{}
	err = json.Unmarshal(body, param)
	if err != nil {
		return nil, err
	}
	return param, nil
}
