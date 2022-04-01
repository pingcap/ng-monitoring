package subscriber

import (
	"context"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/ng-monitoring/component/topology"
	"github.com/pingcap/ng-monitoring/component/topsql/store"
	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/ng-monitoring/config/pdvariable"
	"github.com/pingcap/ng-monitoring/utils"
)

type DefaultSubscriber struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     *sync.WaitGroup
}

func NewDefaultSubscriber(
	topoSubscriber topology.Subscriber,
	varSubscriber pdvariable.Subscriber,
	cfgSubscriber config.Subscriber,
	store store.Store,
) *DefaultSubscriber {
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	wg.Add(1)
	go utils.GoWithRecovery(func() {
		defer wg.Done()
		sm := NewManager(ctx, wg, varSubscriber, topoSubscriber, cfgSubscriber, store)
		sm.Run()
	}, nil)

	return &DefaultSubscriber{
		ctx:    ctx,
		cancel: cancel,
		wg:     wg,
	}
}

var _ Subscriber = &DefaultSubscriber{}

func (ds *DefaultSubscriber) Close() {
	log.Info("stopping scrapers")
	ds.cancel()
	ds.wg.Wait()
	log.Info("stop scrapers successfully")
}
