package query

import (
	"net/http"

	"github.com/zhongzc/ng_monitoring/storage/query/topsql"

	"github.com/genjidb/genji"
)

func Init(vmselectHandler http.HandlerFunc, db *genji.DB) {
	topsql.Init(vmselectHandler, db)
}
