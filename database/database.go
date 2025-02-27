package database

import (
	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/ng-monitoring/database/timeseries"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

func Init(cfg *config.Config) {
	timeseries.Init(cfg)
	// document.Init(cfg)

	log.Info("Initialize database successfully", zap.String("path", cfg.Storage.Path))
}

func Stop() {
	log.Info("Stopping timeseries database")
	timeseries.Stop()
	log.Info("Stop timeseries database successfully")

	// log.Info("Stopping document database")
	// document.Stop()
	// log.Info("Stop document database successfully")
}
