package topsql

import (
	"github.com/gin-gonic/gin"
	"github.com/pingcap/ng-monitoring/component/topsql/service"
	"net/http"

	"github.com/genjidb/genji"
	"github.com/pingcap/ng-monitoring/component/topology"
	"github.com/pingcap/ng-monitoring/component/topsql/query"
	"github.com/pingcap/ng-monitoring/component/topsql/store"
	"github.com/pingcap/ng-monitoring/component/topsql/subscriber"
	"github.com/pingcap/ng-monitoring/config/pdvariable"
)

var (
	defStore      store.Store
	defQuery      query.Query
	defSubscriber subscriber.Subscriber
	defService    *service.Service
)

func Init(
	gj *genji.DB,
	insertHdr, selectHdr http.HandlerFunc,
	topSub topology.Subscriber,
	varSub pdvariable.Subscriber,
) (err error) {
	defStore, err = store.NewDefaultStore(insertHdr, gj)
	if err != nil {
		return err
	}

	defQuery = query.NewDefaultQuery(selectHdr, gj)
	defSubscriber = subscriber.NewDefaultSubscriber(topSub, varSub, defStore)
	defService = service.NewService(defQuery)

	return nil
}

func HTTPService(g *gin.RouterGroup) {
	defService.HTTPService(g)
}

func Stop() {
	defSubscriber.Close()
	defQuery.Close()
	defStore.Close()
}
