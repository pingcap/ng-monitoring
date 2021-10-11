package storage

import (
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"net/http"

	"github.com/zhongzc/diag_backend/storage/database"
	"github.com/zhongzc/diag_backend/storage/database/document"
	"github.com/zhongzc/diag_backend/storage/query"
	"github.com/zhongzc/diag_backend/storage/store"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect"
)

func Init(logPath string, logLevel, dataPath string) {
	database.Init(logPath, logLevel, dataPath)

	store.Init(func(writer http.ResponseWriter, request *http.Request) {
		vminsert.RequestHandler(writer, request)
	}, document.Get())
	query.Init(func(writer http.ResponseWriter, request *http.Request) {
		vmselect.RequestHandler(writer, request)
	}, document.Get())

	log.Info("initialize storage successfully", zap.String("path", dataPath))
}

func Stop() {
	database.Stop()
	log.Info("initialize storage successfully")
}
