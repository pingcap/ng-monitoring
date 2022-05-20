package config

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/types"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	configTableName           = "ng_monitoring_config"
	continuousProfilingModule = "continuous_profiling"
)

var GetDB func() *genji.DB

func LoadConfigFromStorage(getDB func() *genji.DB) error {
	GetDB = getDB
	db := GetDB()
	// create meta table if not exists.
	sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %v (module TEXT primary key, config TEXT)", configTableName)
	err := db.Exec(sql)
	if err != nil {
		return err
	}

	query := fmt.Sprintf("SELECT module, config FROM %v", configTableName)
	res, err := db.Query(query)
	if err != nil {
		return err
	}
	defer res.Close()

	cfgMap := make(map[string]string)
	err = res.Iterate(func(d types.Document) error {
		var module, cfg string
		err = document.Scan(d, &module, &cfg)
		if err != nil {
			return err
		}
		cfgMap[module] = cfg
		return nil
	})
	if err != nil {
		return err
	}
	if len(cfgMap) == 0 {
		return nil
	}

	UpdateGlobalConfig(func(curCfg Config) (res Config) {
		res = curCfg
		for module, cfgStr := range cfgMap {
			switch module {
			case continuousProfilingModule:
				var newCfg ContinueProfilingConfig
				if err = json.NewDecoder(bytes.NewReader([]byte(cfgStr))).Decode(&newCfg); err != nil {
					return
				}
				if newCfg.Valid() {
					res.ContinueProfiling = newCfg
				} else {
					log.Info("load invalid config",
						zap.String("module", module),
						zap.Reflect("module-config", newCfg))
				}
			default:
				err = fmt.Errorf("unknow module config in storage, module: %v, config: %v", module, cfgStr)
				return
			}
			log.Info("load config from storage",
				zap.String("module", module),
				zap.String("module-config", cfgStr),
				zap.Reflect("global-config", res))
		}
		return
	})

	return err
}

func saveConfigIntoStorage() error {
	db := GetDB()
	sql := fmt.Sprintf("DELETE FROM %v", configTableName)
	err := db.Exec(sql)
	if err != nil {
		return err
	}
	sql = fmt.Sprintf("INSERT INTO %v (module, config) VALUES (?, ?)", configTableName)
	cfg := GetGlobalConfig()
	continuousProfilingCfg := cfg.ContinueProfiling
	data, err := json.Marshal(continuousProfilingCfg)
	if err != nil {
		return err
	}
	err = db.Exec(sql, continuousProfilingModule, string(data))
	if err != nil {
		return err
	}
	log.Info("save config into storage",
		zap.String("module", continuousProfilingModule),
		zap.String("config", string(data)))
	return nil
}
