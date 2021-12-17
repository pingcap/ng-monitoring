package subscriber

import (
	"context"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/ng-monitoring/component/topology"
	"github.com/pingcap/ng-monitoring/component/topsql/query"
	"github.com/pingcap/ng-monitoring/component/topsql/store"
	"github.com/pingcap/ng-monitoring/config/pdvariable"
	"github.com/pingcap/ng-monitoring/utils"
)

type Manager struct {
	ctx           context.Context
	wg            *sync.WaitGroup
	enabled       bool
	varSubscriber pdvariable.Subscriber

	components     []topology.Component
	scrapers       map[topology.Component]*Scraper
	topoSubscriber topology.Subscriber

	store store.Store
	query query.Query
}

func NewManager(
	ctx context.Context,
	wg *sync.WaitGroup,
	varSubscriber pdvariable.Subscriber,
	topoSubscriber topology.Subscriber,
	store store.Store,
) *Manager {
	return &Manager{
		ctx:            ctx,
		wg:             wg,
		varSubscriber:  varSubscriber,
		scrapers:       make(map[topology.Component]*Scraper),
		topoSubscriber: topoSubscriber,
		store:          store,
	}
}

func (m *Manager) run() {
	defer func() {
		for _, v := range m.scrapers {
			v.Close()
		}
		m.scrapers = nil
	}()

out:
	for {
		select {
		case vars := <-m.varSubscriber:
			if vars.EnableTopSQL && !m.enabled {
				m.updateScrapers()
				log.Info("Top SQL is enabled")
			}

			if !vars.EnableTopSQL && m.enabled {
				m.clearScrapers()
				log.Info("Top SQL is disabled")
			}

			m.enabled = vars.EnableTopSQL
		case coms := <-m.topoSubscriber:
			if len(coms) == 0 {
				log.Warn("got empty scrapers. Seems to be encountering network problems")
				continue
			}

			m.components = coms
			if m.enabled {
				m.updateScrapers()
			}
		case <-m.ctx.Done():
			break out
		}
	}
}

func (m *Manager) updateScrapers() {
	// clean up closed scrapers
	for component, scraper := range m.scrapers {
		if scraper.IsDown() {
			scraper.Close()
			delete(m.scrapers, component)
		}
	}

	in, out := m.getTopoChange()

	// clean up stale scrapers
	for i := range out {
		m.scrapers[out[i]].Close()
		delete(m.scrapers, out[i])
	}

	// set up incoming scrapers
	for i := range in {
		scraper := NewScraper(m.ctx, in[i], m.store)
		m.scrapers[in[i]] = scraper

		m.wg.Add(1)
		go utils.GoWithRecovery(func() {
			defer m.wg.Done()
			scraper.Run()
		}, nil)
	}
}

func (m *Manager) getTopoChange() (in, out []topology.Component) {
	curMap := make(map[topology.Component]struct{})

	for i := range m.components {
		component := m.components[i]
		switch component.Name {
		case topology.ComponentTiDB:
		case topology.ComponentTiKV:
		default:
			continue
		}

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
		scraper.Close()
		delete(m.scrapers, component)
	}
}
