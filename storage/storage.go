package storage

import (
	"net/http"

	"github.com/zhongzc/diag_backend/storage/database"
	"github.com/zhongzc/diag_backend/storage/database/document"
	"github.com/zhongzc/diag_backend/storage/query"
	"github.com/zhongzc/diag_backend/storage/store"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect"
)

func Init() {
	database.Init()

	store.Init(func(writer http.ResponseWriter, request *http.Request) {
		vminsert.RequestHandler(writer, request)
	}, document.Get())
	query.Init(func(writer http.ResponseWriter, request *http.Request) {
		vmselect.RequestHandler(writer, request)
	}, document.Get())
}

func Stop() {
	database.Stop()
}
