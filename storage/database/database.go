package database

import (
	"github.com/zhongzc/ng_monitoring/config"
	"github.com/zhongzc/ng_monitoring/storage/database/document"
	"github.com/zhongzc/ng_monitoring/storage/database/timeseries"
)

func Init(cfg *config.Config) {
	timeseries.Init(cfg)
	document.Init(cfg)
}

func Stop() {
	timeseries.Stop()
	document.Stop()
}
