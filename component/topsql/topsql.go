package topsql

import (
	"net/http"
	"time"

	"github.com/pingcap/ng-monitoring/component/domain"
	"github.com/pingcap/ng-monitoring/component/subscriber"
	"github.com/pingcap/ng-monitoring/component/topology"
	"github.com/pingcap/ng-monitoring/component/topsql/query"
	"github.com/pingcap/ng-monitoring/component/topsql/service"
	"github.com/pingcap/ng-monitoring/component/topsql/store"
	sub "github.com/pingcap/ng-monitoring/component/topsql/subscriber"
	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/ng-monitoring/config/pdvariable"
	"github.com/pingcap/ng-monitoring/database/docdb"

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
	docDB docdb.DocDB,
	insertHdr, selectHdr http.HandlerFunc,
	topSub topology.Subscriber,
	varSub pdvariable.Subscriber,
	metaRetentionSecs int64,
) (err error) {
	var aggCfg *store.AggregatorConfig
	if tc := config.GetGlobalConfig().TopSQL; tc.AggregateInstances {
		aggCfg = &store.AggregatorConfig{
			FlushHorizon: time.Duration(tc.AggregateFlushHorizonSecs) * time.Second,
			MaxPoints:    tc.AggregateMaxPoints,
		}
	}
	defStore = store.NewDefaultStore(insertHdr, docDB, metaRetentionSecs, aggCfg)
	defQuery = query.NewDefaultQuery(selectHdr, docDB)
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
