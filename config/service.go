package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/pingcap/ng-monitoring/database/docdb"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

func HTTPService(g *gin.RouterGroup, docDB docdb.DocDB) {
	g.GET("", handleGetConfig(docDB))
	g.POST("", handlePostConfig(docDB))
}

func handleGetConfig(docDB docdb.DocDB) gin.HandlerFunc {
	return func(c *gin.Context) {
		cfg := GetGlobalConfig()
		c.JSON(http.StatusOK, cfg)
	}
}

func handlePostConfig(docDB docdb.DocDB) gin.HandlerFunc {
	return func(c *gin.Context) {
		err := handleModifyConfig(c, docDB)
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
}

func handleModifyConfig(c *gin.Context, docDB docdb.DocDB) error {
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
			err := handleContinueProfilingConfigModify(m, docDB)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("config %v not support modify or unknow", k)
		}
	}
	return nil
}

func handleContinueProfilingConfigModify(reqNested map[string]interface{}, docDB docdb.DocDB) (err error) {
	UpdateGlobalConfig(func(curCfg Config) (res Config) {
		res = curCfg
		var current []byte
		current, err = json.Marshal(curCfg.ContinueProfiling)
		if err != nil {
			return
		}

		var currentNested map[string]interface{}
		if err = json.NewDecoder(bytes.NewReader(current)).Decode(&currentNested); err != nil {
			return
		}

		for k, newValue := range reqNested {
			oldValue, ok := currentNested[k]
			if !ok {
				err = fmt.Errorf("unknown config `%v`", k)
				return
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

		var data []byte
		data, err = json.Marshal(currentNested)
		if err != nil {
			return
		}
		var newCfg ContinueProfilingConfig
		err = json.NewDecoder(bytes.NewReader(data)).Decode(&newCfg)
		if err != nil {
			return
		}

		if !newCfg.Valid() {
			err = fmt.Errorf("new config is invalid: %v", string(data))
			return
		}
		res.ContinueProfiling = newCfg
		return
	})

	if err != nil {
		return err
	}

	return saveConfigIntoStorage(docDB)
}
