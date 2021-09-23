package database

import (
	"github.com/zhongzc/diag_backend/storage/database/document"
	"github.com/zhongzc/diag_backend/storage/database/timeseries"
)

func Init() {
	document.Init()
	timeseries.Init()
}

func Stop() {
	timeseries.Stop()
	document.Stop()
}
