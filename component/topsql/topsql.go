package topsql

import (
	"net/http"

	"github.com/pingcap/ng_monitoring/component/topology"
	"github.com/pingcap/ng_monitoring/component/topsql/query"
	"github.com/pingcap/ng_monitoring/component/topsql/store"
	"github.com/pingcap/ng_monitoring/component/topsql/subscriber"

	"github.com/genjidb/genji"
)

func Init(gj *genji.DB, insertHdr, selectHdr http.HandlerFunc, subsbr topology.Subscriber) {
	store.Init(insertHdr, gj)
	query.Init(selectHdr, gj)
	subscriber.Init(subsbr)
}

func Stop() {
	subscriber.Stop()
	store.Stop()
	query.Stop()
}
