package scrape

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/ng-monitoring/component/conprof/meta"
	"github.com/pingcap/ng-monitoring/component/conprof/store"
	"github.com/pingcap/ng-monitoring/component/topology"
	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/ng-monitoring/utils"

	"github.com/pingcap/log"
	commonconfig "github.com/prometheus/common/config"
	"go.uber.org/zap"
)

var (
	updateTargetMetaInterval = time.Minute
)

// Manager maintains a set of scrape pools and manages start/stop cycles
// when receiving new target groups form the discovery manager.
type Manager struct {
	store           *store.ProfileStorage
	topoSubScribe   topology.Subscriber
	latestTopoComps map[topology.Component]struct{}

	config         config.Config
	configChangeCh config.Subscriber

	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu                  sync.Mutex
	runningScrapeSuites map[topology.Component][]*ScrapeSuite
	ticker              *Ticker
}

// NewManager is the Manager constructor
func NewManager(store *store.ProfileStorage, topoSubScribe topology.Subscriber) *Manager {
	cfgSub := config.Subscribe()
	getCurCfg := <-cfgSub
	cfg := getCurCfg()

	return &Manager{
		store:               store,
		topoSubScribe:       topoSubScribe,
		config:              cfg,
		configChangeCh:      cfgSub,
		latestTopoComps:     map[topology.Component]struct{}{},
		runningScrapeSuites: make(map[topology.Component][]*ScrapeSuite),
		ticker:              NewTicker(time.Duration(cfg.ContinueProfiling.IntervalSeconds) * time.Second),
	}
}

func (m *Manager) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel
	go utils.GoWithRecovery(func() {
		m.run(ctx)
	}, nil)

	go utils.GoWithRecovery(func() {
		m.updateTargetMetaLoop(ctx)
	}, nil)
	log.Info("continuous profiling manager started")
}

func (m *Manager) GetCurrentScrapeComponents() []topology.Component {
	components := make([]topology.Component, 0, len(m.runningScrapeSuites))
	m.mu.Lock()
	defer m.mu.Unlock()
	for comp := range m.runningScrapeSuites {
		components = append(components, comp)
	}
	sort.Slice(components, func(i, j int) bool {
		if components[i].Name != components[j].Name {
			return components[i].Name < components[j].Name
		}
		if components[i].IP != components[j].IP {
			return components[i].IP < components[j].IP
		}
		return components[i].Port < components[j].Port
	})
	return components
}

func (m *Manager) updateTargetMetaLoop(ctx context.Context) {
	ticker := time.NewTicker(updateTargetMetaInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.updateTargetMeta()
		}
	}
}

func (m *Manager) updateTargetMeta() {
	suites := m.GetAllCurrentScrapeSuite()
	count := 0
	for _, suite := range suites {
		ts := suite.lastScrape.Unix()
		if ts <= 0 {
			continue
		}
		target := suite.scraper.target.ProfileTarget
		updated, err := m.store.UpdateProfileTargetInfo(target, ts)
		if err != nil {
			log.Error("update profile target info failed",
				zap.String("component", target.Component),
				zap.String("kind", target.Kind),
				zap.String("address", target.Address),
				zap.Error(err))
		} else if updated {
			count++
		}
	}
	log.Debug("update profile target info finished", zap.Int("update-count", count))
}

func (m *Manager) run(ctx context.Context) {
	buildMap := func(components []topology.Component) map[topology.Component]struct{} {
		m := make(map[topology.Component]struct{}, len(components))
		for _, comp := range components {
			m[comp] = struct{}{}
		}
		return m
	}
	oldCfg := m.config.ContinueProfiling
	for {
		select {
		case getCfg := <-m.configChangeCh:
			m.config = getCfg()
		case getComponents := <-m.topoSubScribe:
			components := getComponents()
			m.latestTopoComps = buildMap(components)
		case <-ctx.Done():
			return
		}

		m.reload(ctx, oldCfg, m.config.ContinueProfiling)
		oldCfg = m.config.ContinueProfiling
	}
}

func (m *Manager) isProfilingConfigChanged(oldCfg, newCfg config.ContinueProfilingConfig) bool {
	return oldCfg.Enable != newCfg.Enable ||
		oldCfg.ProfileSeconds != newCfg.ProfileSeconds
}

func (m *Manager) reload(ctx context.Context, oldCfg, newCfg config.ContinueProfilingConfig) {
	if oldCfg.IntervalSeconds != newCfg.IntervalSeconds {
		m.ticker.Reset(time.Second * time.Duration(newCfg.IntervalSeconds))
	}

	needReload := m.isProfilingConfigChanged(oldCfg, newCfg)
	// close for old components
	comps := m.getComponentNeedStopScrape(needReload)
	for _, comp := range comps {
		m.stopScrape(comp)
	}

	if !newCfg.Enable {
		return
	}

	// start for new component.
	comps = m.getComponentNeedStartScrape(needReload)
	for _, comp := range comps {
		err := m.startScrape(ctx, comp, newCfg)
		if err != nil {
			log.Error("start scrape failed",
				zap.String("component", comp.Name),
				zap.String("address", comp.IP+":"+strconv.Itoa(int(comp.StatusPort))))
		}
	}
}

func (m *Manager) startScrape(ctx context.Context, component topology.Component, continueProfilingCfg config.ContinueProfilingConfig) error {
	if !continueProfilingCfg.Enable {
		return nil
	}
	// TODO: remove this after TiFlash fix the profile bug.
	if component.Name == topology.ComponentTiFlash {
		return nil
	}
	profilingConfig := m.getProfilingConfig(component)
	if profilingConfig == nil {
		return nil
	}
	httpCfg := m.config.Security.GetHTTPClientConfig()
	addr := fmt.Sprintf("%v:%v", component.IP, component.Port)
	scrapeAddr := fmt.Sprintf("%v:%v", component.IP, component.StatusPort)
	for profileName, profileConfig := range profilingConfig.PprofConfig {
		target := NewTarget(component.Name, addr, scrapeAddr, profileName, m.config.GetHTTPScheme(), profileConfig)
		client, err := commonconfig.NewClientFromConfig(httpCfg, component.Name)
		if err != nil {
			return err
		}
		scrape := newScraper(target, client)
		scrapeSuite := newScrapeSuite(ctx, scrape, m.store)
		m.wg.Add(1)
		go utils.GoWithRecovery(func() {
			defer m.wg.Done()
			scrapeSuite.run(m.ticker.Subscribe())
		}, nil)
		m.addScrapeSuite(component, scrapeSuite)
	}
	log.Info("start component scrape",
		zap.String("component", component.Name),
		zap.String("address", addr))
	return nil
}

func (m *Manager) stopScrape(component topology.Component) {
	addr := fmt.Sprintf("%v:%v", component.IP, component.StatusPort)
	log.Info("stop component scrape",
		zap.String("component", component.Name),
		zap.String("address", addr))
	suites := m.deleteScrapeSuite(component)
	for _, suite := range suites {
		if suite == nil {
			continue
		}
		suite.stop()
	}
}

func (m *Manager) getProfilingConfig(component topology.Component) *config.ProfilingConfig {
	switch component.Name {
	case topology.ComponentTiDB, topology.ComponentPD, topology.ComponentTiCDC:
		return goAppProfilingConfig(m.config.ContinueProfiling)
	case topology.ComponentTiKV:
		return tikvProfilingConfig(m.config.ContinueProfiling)
	case topology.ComponentTiFlash:
		return tiflashProfilingConfig(m.config.ContinueProfiling)
	default:
		return nil
	}
}

func (m *Manager) getComponentNeedStopScrape(needReload bool) []topology.Component {
	comps := []topology.Component{}
	m.mu.Lock()
	for comp := range m.runningScrapeSuites {
		_, exist := m.latestTopoComps[comp]
		if exist && !needReload {
			continue
		}
		comps = append(comps, comp)
	}
	m.mu.Unlock()
	return comps
}

func (m *Manager) getComponentNeedStartScrape(needReload bool) []topology.Component {
	comps := []topology.Component{}
	m.mu.Lock()
	for comp := range m.latestTopoComps {
		_, exist := m.runningScrapeSuites[comp]
		if exist && !needReload {
			continue
		}
		comps = append(comps, comp)
	}
	m.mu.Unlock()
	return comps
}

func (m *Manager) addScrapeSuite(component topology.Component, suite *ScrapeSuite) {
	m.mu.Lock()
	m.runningScrapeSuites[component] = append(m.runningScrapeSuites[component], suite)
	m.mu.Unlock()
}

func (m *Manager) deleteScrapeSuite(component topology.Component) []*ScrapeSuite {
	m.mu.Lock()
	suites := m.runningScrapeSuites[component]
	delete(m.runningScrapeSuites, component)
	m.mu.Unlock()
	return suites
}

func (m *Manager) GetAllCurrentScrapeSuite() []*ScrapeSuite {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]*ScrapeSuite, 0, len(m.runningScrapeSuites))
	for _, suites := range m.runningScrapeSuites {
		result = append(result, suites...)
	}
	return result
}

func (m *Manager) GetLastScrapeTime() time.Time {
	return m.ticker.lastTime
}

func (m *Manager) GetRunningStatus() meta.ProfileStatus {
	var statusCounter meta.StatusCounter
	m.mu.Lock()
	for _, scrapeSuites := range m.runningScrapeSuites {
		for _, scrapeSuite := range scrapeSuites {
			status := scrapeSuite.lastScrapeStatus.Load()
			statusCounter.AddStatus(meta.ProfileStatus(status))
		}
	}
	m.mu.Unlock()
	return statusCounter.GetFinalStatus()
}

func (m *Manager) Close() {
	if m.cancel != nil {
		m.cancel()
	}
	if m.ticker != nil {
		m.ticker.Stop()
	}
	m.store.Close()
	m.wg.Wait()
}

func goAppProfilingConfig(cfg config.ContinueProfilingConfig) *config.ProfilingConfig {
	return &config.ProfilingConfig{
		PprofConfig: config.PprofConfig{
			"heap": &config.PprofProfilingConfig{
				Path: "/debug/pprof/heap",
			},
			"goroutine": &config.PprofProfilingConfig{
				Path: "/debug/pprof/goroutine",
				// debug=2 causes STW when collecting the stacks. See https://github.com/pingcap/tidb/issues/48695.
				Params: map[string]string{"debug": "1"},
			},
			"mutex": &config.PprofProfilingConfig{
				Path: "/debug/pprof/mutex",
			},
			"profile": &config.PprofProfilingConfig{
				Path:    "/debug/pprof/profile",
				Seconds: cfg.ProfileSeconds,
			},
		},
	}
}

func tikvProfilingConfig(cfg config.ContinueProfilingConfig) *config.ProfilingConfig {
	return &config.ProfilingConfig{
		PprofConfig: config.PprofConfig{
			"profile": &config.PprofProfilingConfig{
				Path:    "/debug/pprof/profile",
				Seconds: cfg.ProfileSeconds,
				Header:  map[string]string{"Content-Type": "application/protobuf"},
			},
			"heap": &config.PprofProfilingConfig{
				Path: "/debug/pprof/heap",
			},
		},
	}
}

func tiflashProfilingConfig(cfg config.ContinueProfilingConfig) *config.ProfilingConfig {
	return &config.ProfilingConfig{
		PprofConfig: config.PprofConfig{
			"profile": &config.PprofProfilingConfig{
				Path:    "/debug/pprof/profile",
				Seconds: cfg.ProfileSeconds,
				Header:  map[string]string{"Content-Type": "application/protobuf"},
			},
		},
	}
}
