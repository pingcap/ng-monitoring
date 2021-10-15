package topsql

import (
	"net/http"

	"github.com/genjidb/genji"
	"github.com/zhongzc/ng_monitoring/component/topsql/query"
	"github.com/zhongzc/ng_monitoring/component/topsql/store"
)

func Init(gj *genji.DB, insertHdr, selectHdr http.HandlerFunc) {
	store.Init(insertHdr, gj)
	query.Init(selectHdr, gj)
}

func Stop() {
	store.Stop()
	query.Stop()
}
