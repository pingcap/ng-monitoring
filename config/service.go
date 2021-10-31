package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

func HTTPService(g *gin.RouterGroup) {
	g.GET("", handleGetConfig)
	g.POST("", handlePostConfig)
}

func handleGetConfig(c *gin.Context) {
	cfg := GetGlobalConfig()
	c.JSON(http.StatusOK, cfg)
}

func handlePostConfig(c *gin.Context) {
	err := handleModifyConfig(c)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status":  "error",
			"message": err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"status": "ok",
	})
}

func handleModifyConfig(c *gin.Context) error {
	var reqNested map[string]interface{}
	if err := json.NewDecoder(c.Request.Body).Decode(&reqNested); err != nil {
		return err
	}
	for k, v := range reqNested {
		switch k {
		case "continuous_profiling":
			m, ok := v.(map[string]interface{})
			if !ok {
				return fmt.Errorf("%v config value is invalid: %v", k, v)
			}
			return handleContinueProfilingConfigModify(m)
		default:
			return fmt.Errorf("config %v not support modify or unknow", k)
		}
	}
	return nil
}

func handleContinueProfilingConfigModify(reqNested map[string]interface{}) error {
	cfg := GetGlobalConfig()
	current, err := json.Marshal(cfg.ContinueProfiling)
	if err != nil {
		return err
	}

	var currentNested map[string]interface{}
	if err := json.NewDecoder(bytes.NewReader(current)).Decode(&currentNested); err != nil {
		return err
	}

	for k, newValue := range reqNested {
		oldValue, ok := currentNested[k]
		if !ok {
			return fmt.Errorf("unknow config `%v`", k)
		}
		if oldValue == newValue {
			continue
		}
		currentNested[k] = newValue
		log.Info("handle continuous profiling config modify",
			zap.String("name", k),
			zap.Reflect("old-value", oldValue),
			zap.Reflect("new-value", newValue))
	}

	data, err := json.Marshal(currentNested)
	if err != nil {
		return err
	}
	var newCfg ContinueProfilingConfig
	err = json.NewDecoder(bytes.NewReader(data)).Decode(&newCfg)
	if err != nil {
		return err
	}

	if !newCfg.Valid() {
		return fmt.Errorf("new config is invalid: %v", string(data))
	}
	cfg.ContinueProfiling = newCfg
	StoreGlobalConfig(cfg)
	return saveConfigIntoStorage()
}
