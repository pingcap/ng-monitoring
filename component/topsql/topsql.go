package topsql

import (
	"net/http"

	"github.com/pingcap/ng-monitoring/component/domain"
	"github.com/pingcap/ng-monitoring/component/subscriber"
	"github.com/pingcap/ng-monitoring/component/topology"
	"github.com/pingcap/ng-monitoring/component/topsql/query"
	"github.com/pingcap/ng-monitoring/component/topsql/service"
	"github.com/pingcap/ng-monitoring/component/topsql/store"
	sub "github.com/pingcap/ng-monitoring/component/topsql/subscriber"
	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/ng-monitoring/config/pdvariable"

	"github.com/genjidb/genji"
	"github.com/gin-gonic/gin"
)

var (
	defStore      store.Store
	defQuery      query.Query
	defSubscriber *subscriber.Subscriber
	defService    *service.Service
)

func Init(
	do *domain.Domain,
	cfgSub config.Subscriber,
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
	defSubscriber = sub.NewSubscriber(topSub, varSub, cfgSub, do, defStore)
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
