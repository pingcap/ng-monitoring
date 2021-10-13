package store

import (
	"net/http"

	"github.com/zhongzc/ng_monitoring/storage/store/topsql"

	"github.com/genjidb/genji"
)

func Init(vminsertHandler http.HandlerFunc, documentDB *genji.DB) {
	topsql.Init(vminsertHandler, documentDB)
}
