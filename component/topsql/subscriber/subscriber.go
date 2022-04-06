package subscriber

import (
	"context"
	"fmt"
	"time"

	"github.com/pingcap/ng-monitoring/component/subscriber"
	"github.com/pingcap/ng-monitoring/component/topology"
	"github.com/pingcap/ng-monitoring/component/topsql/store"
	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/ng-monitoring/config/pdvariable"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

var (
	instancesItemSliceP = &instancesItemSlicePool{}
)

func NewSubscriber(
	topoSubscriber topology.Subscriber,
	varSubscriber pdvariable.Subscriber,
	cfgSubscriber config.Subscriber,
	store store.Store,
) *subscriber.Subscriber {
	controller := NewSubscriberController(store)
	return subscriber.NewSubscriber(
		topoSubscriber,
		varSubscriber,
		cfgSubscriber,
		controller,
	)
}

type SubscriberController struct {
	store store.Store

	config     *config.Config
	variable   *pdvariable.PDVariable
	components []topology.Component
}

func NewSubscriberController(store store.Store) *SubscriberController {
	cfg := config.GetDefaultConfig()
	variable := pdvariable.DefaultPDVariable()
	return &SubscriberController{
		store:    store,
		config:   &cfg,
		variable: variable,
	}
}

var _ subscriber.SubscribeController = &SubscriberController{}

func (sc *SubscriberController) NewScraper(ctx context.Context, component topology.Component) subscriber.Scraper {
	return NewScraper(ctx, component, sc.store, sc.config.Security.GetTLSConfig())
}

func (sc *SubscriberController) Name() string {
	return "Top SQL"
}

func (sc *SubscriberController) IsEnabled() bool {
	return sc.variable.EnableTopSQL
}

func (sc *SubscriberController) UpdatePDVariable(variable pdvariable.PDVariable) {
	sc.variable = &variable
}

func (sc *SubscriberController) UpdateConfig(cfg config.Config) {
	sc.config = &cfg
}

func (sc *SubscriberController) UpdateTopology(components []topology.Component) {
	sc.components = components

	if sc.variable.EnableTopSQL {
		if err := sc.storeTopology(); err != nil {
			log.Warn("failed to store topology", zap.Error(err))
		}
	}
}

func (sc *SubscriberController) storeTopology() error {
	if len(sc.components) == 0 {
		return nil
	}

	items := instancesItemSliceP.Get()
	defer instancesItemSliceP.Put(items)

	now := time.Now().Unix()
	for _, com := range sc.components {
		switch com.Name {
		case topology.ComponentTiDB:
			*items = append(*items, store.InstanceItem{
				Instance:     fmt.Sprintf("%s:%d", com.IP, com.StatusPort),
				InstanceType: topology.ComponentTiDB,
				TimestampSec: uint64(now),
			})
		case topology.ComponentTiKV:
			*items = append(*items, store.InstanceItem{
				Instance:     fmt.Sprintf("%s:%d", com.IP, com.Port),
				InstanceType: topology.ComponentTiKV,
				TimestampSec: uint64(now),
			})
		}
	}
	return sc.store.Instances(*items)
}
