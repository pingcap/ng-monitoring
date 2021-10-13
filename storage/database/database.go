package database

import (
	"github.com/zhongzc/ng_monitoring_server/config"
	"github.com/zhongzc/ng_monitoring_server/storage/database/document"
	"github.com/zhongzc/ng_monitoring_server/storage/database/timeseries"
)

func Init(cfg *config.Config) {
	timeseries.Init(cfg)
	document.Init(cfg)
}

func Stop() {
	timeseries.Stop()
	document.Stop()
}
