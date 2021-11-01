package database

import (
	"github.com/pingcap/ng_monitoring/config"
	"github.com/pingcap/ng_monitoring/database/document"
	"github.com/pingcap/ng_monitoring/database/timeseries"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

func Init(cfg *config.Config) {
	timeseries.Init(cfg)
	document.Init(cfg)

	log.Info("Initialize database successfully", zap.String("path", cfg.Storage.Path))
}

func Stop() {
	log.Info("Stopping timeserires database")
	timeseries.Stop()
	log.Info("Stop timeserires database successfully")

	log.Info("Stopping document database")
	document.Stop()
	log.Info("Stop document database successfully")
}
