package storage

import (
	"net/http"

	"github.com/zhongzc/ng_monitoring_server/config"
	"github.com/zhongzc/ng_monitoring_server/storage/database"
	"github.com/zhongzc/ng_monitoring_server/storage/database/document"
	"github.com/zhongzc/ng_monitoring_server/storage/query"
	"github.com/zhongzc/ng_monitoring_server/storage/store"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

func Init(cfg *config.Config) {
	database.Init(cfg)

	store.Init(func(writer http.ResponseWriter, request *http.Request) {
		vminsert.RequestHandler(writer, request)
	}, document.Get())
	query.Init(func(writer http.ResponseWriter, request *http.Request) {
		vmselect.RequestHandler(writer, request)
	}, document.Get())

	log.Info("initialize storage successfully", zap.String("path", cfg.Storage.Path))
}

func Stop() {
	database.Stop()
	log.Info("stop storage successfully")
}
