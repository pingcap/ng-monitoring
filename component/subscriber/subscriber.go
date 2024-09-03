package subscriber

import (
	"context"
	"fmt"
	"sync"

	"github.com/pingcap/ng-monitoring/component/domain"
	"github.com/pingcap/ng-monitoring/component/topology"
	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/ng-monitoring/config/pdvariable"
	"github.com/pingcap/ng-monitoring/utils"

	"github.com/pingcap/log"
)

type Subscriber struct {
	ctx                 context.Context
	cancel              context.CancelFunc
	wg                  *sync.WaitGroup
	subscribeController SubscribeController
}

func NewSubscriber(
	do *domain.Domain,
	topoSubscriber topology.Subscriber,
	varSubscriber pdvariable.Subscriber,
	cfgSubscriber config.Subscriber,
	subscribeController SubscribeController,
) *Subscriber {
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	wg.Add(1)
	sm := NewManager(ctx, wg, do, varSubscriber, topoSubscriber, cfgSubscriber, subscribeController)
	go utils.GoWithRecovery(func() {
		defer wg.Done()
		sm.Run()
	}, nil)

	return &Subscriber{
		ctx:                 ctx,
		cancel:              cancel,
		wg:                  wg,
		subscribeController: subscribeController,
	}
}

func (s *Subscriber) Close() {
	log.Info(fmt.Sprintf("stopping %s scrapers", s.subscribeController.Name()))
	s.cancel()
	s.wg.Wait()
	log.Info(fmt.Sprintf("stop %s scrapers successfully", s.subscribeController.Name()))
}
