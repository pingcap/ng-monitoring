package subscriber

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/pingcap/ng-monitoring/component/topology"
	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/ng-monitoring/config/pdvariable"
	"github.com/pingcap/ng-monitoring/utils"

	"github.com/pingcap/log"
)

type Manager struct {
	ctx context.Context
	wg  *sync.WaitGroup

	prevEnabled bool
	components  []topology.Component

	scrapers map[topology.Component]Scraper

	topoSubscriber topology.Subscriber
	cfgSubscriber  config.Subscriber
	varSubscriber  pdvariable.Subscriber

	subscribeController SubscribeController
}

func NewManager(
	ctx context.Context,
	wg *sync.WaitGroup,
	varSubscriber pdvariable.Subscriber,
	topoSubscriber topology.Subscriber,
	cfgSubscriber config.Subscriber,
	subscribeController SubscribeController,
) *Manager {
	return &Manager{
		ctx: ctx,
		wg:  wg,

		scrapers: make(map[topology.Component]Scraper),

		varSubscriber:  varSubscriber,
		topoSubscriber: topoSubscriber,
		cfgSubscriber:  cfgSubscriber,

		prevEnabled:         subscribeController.IsEnabled(),
		subscribeController: subscribeController,
	}
}

func (m *Manager) Run() {
	defer m.clearScrapers()

	for {
		select {
		case getCfg := <-m.cfgSubscriber:
			m.subscribeController.UpdateConfig(getCfg())
		case getVars := <-m.varSubscriber:
			m.subscribeController.UpdatePDVariable(getVars())
		case getTopology := <-m.topoSubscriber:
			m.components = getTopology()
			m.subscribeController.UpdateTopology(getTopology())
		case <-m.ctx.Done():
			return
		}

		curEnabled := m.subscribeController.IsEnabled()
		if curEnabled != m.prevEnabled { // switch
			action := "off"
			if curEnabled {
				action = "on"
			}
			log.Info(fmt.Sprintf("%s is turned %s", m.subscribeController.Name(), action))
		}
		m.prevEnabled = curEnabled

		if curEnabled {
			m.updateScrapers()
		} else {
			m.clearScrapers()
		}
	}
}

func (m *Manager) updateScrapers() {
	// clean up closed scrapers
	for component, scraper := range m.scrapers {
		if !isNil(scraper) && scraper.IsDown() {
			scraper.Close()
			delete(m.scrapers, component)
		}
	}

	in, out := m.getTopoChange()

	// clean up stale scrapers
	for i := range out {
		scraper := m.scrapers[out[i]]
		if !isNil(scraper) {
			scraper.Close()
		}
		delete(m.scrapers, out[i])
	}

	// set up incoming scrapers
	for i := range in {
		scraper := m.subscribeController.NewScraper(m.ctx, in[i])
		m.scrapers[in[i]] = scraper

		if !isNil(scraper) {
			m.wg.Add(1)
			go utils.GoWithRecovery(func() {
				defer m.wg.Done()
				scraper.Run()
			}, nil)
		}
	}
}

func (m *Manager) getTopoChange() (in, out []topology.Component) {
	curMap := make(map[topology.Component]struct{})

	for i := range m.components {
		component := m.components[i]
		curMap[component] = struct{}{}
		if _, contains := m.scrapers[component]; !contains {
			in = append(in, component)
		}
	}

	for c := range m.scrapers {
		if _, contains := curMap[c]; !contains {
			out = append(out, c)
		}
	}

	return
}

func (m *Manager) clearScrapers() {
	for component, scraper := range m.scrapers {
		if !isNil(scraper) {
			scraper.Close()
		}
		delete(m.scrapers, component)
	}
}

func isNil(scraper Scraper) bool {
	if scraper == nil {
		return true
	}
	switch reflect.TypeOf(scraper).Kind() {
	case reflect.Ptr, reflect.Map, reflect.Array, reflect.Chan, reflect.Slice:
		return reflect.ValueOf(scraper).IsNil()
	}
	return false
}
