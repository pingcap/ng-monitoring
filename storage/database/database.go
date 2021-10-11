package database

import (
	"github.com/zhongzc/diag_backend/storage/database/document"
	"github.com/zhongzc/diag_backend/storage/database/timeseries"
	"path"
)

func Init(logPath string, logLevel string, dataPath string) {
	timeseries.Init(path.Join(logPath, "tsdb.log"), logLevel, path.Join(dataPath, "tsdb"))
	document.Init(path.Join(logPath, "docdb.log"), logLevel, path.Join(dataPath, "docdb"))
}

func Stop() {
	timeseries.Stop()
	document.Stop()
}
