package subscriber

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ng-monitoring/component/topology"
	"github.com/pingcap/ng-monitoring/component/topsql/store"
	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/ng-monitoring/config/pdvariable"
	"github.com/pingcap/ng-monitoring/utils"
	"go.uber.org/zap"
)

var (
	instancesP = instancesItemSlicePool{}
)

type Manager struct {
	ctx    context.Context
	cancel context.CancelFunc

	wg            *sync.WaitGroup
	enabled       bool
	varSubscriber pdvariable.Subscriber

	components     []topology.Component
	scrapers       map[topology.Component]*Scraper
	topoSubscriber topology.Subscriber

	config        *config.Config
	cfgSubscriber config.Subscriber

	store store.Store
}

func NewManager(
	ctx context.Context,
	wg *sync.WaitGroup,
	cfg *config.Config,
	varSubscriber pdvariable.Subscriber,
	topoSubscriber topology.Subscriber,
	cfgSubscriber config.Subscriber,
	store store.Store,
) *Manager {
	ctx, cancel := context.WithCancel(ctx)
	return &Manager{
		ctx:            ctx,
		cancel:         cancel,
		wg:             wg,
		varSubscriber:  varSubscriber,
		scrapers:       make(map[topology.Component]*Scraper),
		topoSubscriber: topoSubscriber,

		config:        cfg,
		cfgSubscriber: cfgSubscriber,

		store: store,
	}
}

func (m *Manager) Run() {
	storeTopoInterval := time.NewTicker(30 * time.Second)
	defer func() {
		storeTopoInterval.Stop()
		for _, v := range m.scrapers {
			v.Close()
		}
		m.scrapers = nil
	}()

	for {
		if m.enabled {
			if err := m.storeTopology(); err != nil {
				log.Warn("failed to store topology", zap.Error(err))
			}
		}

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
		case <-storeTopoInterval.C:
			continue
		case cfg := <-m.cfgSubscriber:
			m.config = cfg
		case <-m.ctx.Done():
			return
		}
	}
}

func (m *Manager) Close() {
	m.cancel()
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
		scraper := NewScraper(m.ctx, in[i], m.store, m.config.Security.GetTLSConfig())
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

func (m *Manager) storeTopology() error {
	if len(m.components) == 0 {
		return nil
	}

	items := instancesP.Get()
	defer instancesP.Put(items)

	now := time.Now().Unix()
	for _, com := range m.components {
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
	return m.store.Instances(*items)
}
