package subscriber

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/pingcap/ng-monitoring/component/domain"
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
	domain *domain.Domain,
	store store.Store,
) *subscriber.Subscriber {
	controller := NewSubscriberController(store)
	return subscriber.NewSubscriber(
		domain,
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

func (sc *SubscriberController) NewScraper(ctx context.Context, component topology.Component, schemaInfo *sync.Map) subscriber.Scraper {
	return NewScraper(ctx, schemaInfo, component, sc.store, sc.config.Security.GetTLSConfig())
}

func (sc *SubscriberController) NewHTTPClient() *http.Client {
	dialer := &net.Dialer{
		Timeout: 10 * time.Second,
	}
	tr := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		MaxIdleConnsPerHost:   runtime.GOMAXPROCS(0) + 1,
		TLSClientConfig:       sc.config.Security.GetTLSConfig(),
	}
	return &http.Client{
		Transport: tr,
	}
}

func (sc *SubscriberController) Name() string {
	return "Top SQL"
}

func (sc *SubscriberController) GetConfig() *config.Config {
	return sc.config
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
