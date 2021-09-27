package database

import (
	"github.com/zhongzc/diag_backend/storage/database/document"
	"github.com/zhongzc/diag_backend/storage/database/timeseries"
)

func Init(tsdbPath string, docdbPath string) {
	timeseries.Init(tsdbPath)
	document.Init(docdbPath)
}

func Stop() {
	timeseries.Stop()
	document.Stop()
}
