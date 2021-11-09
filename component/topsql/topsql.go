package topsql

import (
	"github.com/pingcap/ng_monitoring/config/pdvariable"
	"net/http"

	"github.com/pingcap/ng_monitoring/component/topology"
	"github.com/pingcap/ng_monitoring/component/topsql/query"
	"github.com/pingcap/ng_monitoring/component/topsql/store"
	"github.com/pingcap/ng_monitoring/component/topsql/subscriber"

	"github.com/genjidb/genji"
)

func Init(gj *genji.DB, insertHdr, selectHdr http.HandlerFunc, topSub topology.Subscriber, varSub pdvariable.Subscriber) {
	store.Init(insertHdr, gj)
	query.Init(selectHdr, gj)
	subscriber.Init(topSub, varSub)
}

func Stop() {
	subscriber.Stop()
	store.Stop()
	query.Stop()
}
