package config

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/pingcap/log"
	"github.com/pingcap/ng-monitoring/database/docdb"
	"go.uber.org/zap"
)

const (
	continuousProfilingModule = "continuous_profiling"
)

func LoadConfigFromStorage(ctx context.Context, db docdb.DocDB) error {
	cfgMap, err := db.LoadConfig(ctx)
	if err != nil {
		return err
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

func saveConfigIntoStorage(db docdb.DocDB) error {
	cfg := GetGlobalConfig()
	continuousProfilingCfg := cfg.ContinueProfiling
	data, err := json.Marshal(continuousProfilingCfg)
	if err != nil {
		return err
	}
	return db.SaveConfig(context.Background(), map[string]string{
		continuousProfilingModule: string(data),
	})
}
