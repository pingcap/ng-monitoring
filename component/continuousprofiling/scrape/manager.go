package scrape

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/log"
	commonconfig "github.com/prometheus/common/config"
	"github.com/zhongzc/ng_monitoring/component/continuousprofiling/meta"
	"github.com/zhongzc/ng_monitoring/component/continuousprofiling/store"
	"github.com/zhongzc/ng_monitoring/component/continuousprofiling/util"
	"github.com/zhongzc/ng_monitoring/component/topology"
	"github.com/zhongzc/ng_monitoring/config"
	"github.com/zhongzc/ng_monitoring/utils"
	"go.uber.org/zap"
)

// Manager maintains a set of scrape pools and manages start/stop cycles
// when receiving new target groups form the discovery manager.
type Manager struct {
	store          *store.ProfileStorage
	topoSubScribe  topology.Subscriber
	reloadCh       chan struct{}
	curComponents  map[topology.Component]struct{}
	lastComponents map[topology.Component]struct{}

	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu           sync.Mutex
	scrapeSuites map[meta.ProfileTarget]*ScrapeSuite
}

// NewManager is the Manager constructor
func NewManager(store *store.ProfileStorage, topoSubScribe topology.Subscriber) *Manager {
	return &Manager{
		store:          store,
		topoSubScribe:  topoSubScribe,
		reloadCh:       make(chan struct{}, 10),
		curComponents:  map[topology.Component]struct{}{},
		lastComponents: map[topology.Component]struct{}{},
		scrapeSuites:   make(map[meta.ProfileTarget]*ScrapeSuite),
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

func (m *Manager) NotifyReload() {
	select {
	case m.reloadCh <- struct{}{}:
	default:
	}
}

func (m *Manager) GetCurrentScrapeComponents() []topology.Component {
	components := make([]topology.Component, 0, len(m.curComponents))
	for comp := range m.curComponents {
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
	ticker := time.NewTicker(time.Minute)
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
	targets, suites := m.GetAllCurrentScrapeSuite()
	count := 0
	for i, suite := range suites {
		ts := util.GetTimeStamp(suite.lastScrape)
		if ts <= 0 {
			continue
		}
		target := targets[i]
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
	log.Info("update profile target info finished", zap.Int("update-count", count))
}

func (m *Manager) run(ctx context.Context) {
	buildMap := func(components []topology.Component) map[topology.Component]struct{} {
		m := make(map[topology.Component]struct{}, len(components))
		for _, comp := range components {
			m[comp] = struct{}{}
		}
		return m
	}
	oldCfg := config.GetGlobalConfig().ContinueProfiling
	for {
		select {
		case <-ctx.Done():
			return
		case components := <-m.topoSubScribe:
			m.lastComponents = buildMap(components)
		case <-m.reloadCh:
			break
		}

		newCfg := config.GetGlobalConfig().ContinueProfiling
		m.reload(ctx, oldCfg, newCfg)
		oldCfg = newCfg
	}
}

func (m *Manager) reload(ctx context.Context, oldCfg, newCfg config.ContinueProfilingConfig) {
	configChanged := oldCfg != newCfg
	// close for old components
	for comp := range m.curComponents {
		_, exist := m.lastComponents[comp]
		if exist && !configChanged {
			continue
		}
		m.stopScrape(comp)
	}

	// close for old components
	if !newCfg.Enable {
		return
	}

	//start for new component.
	for comp := range m.lastComponents {
		_, exist := m.curComponents[comp]
		if exist && !configChanged {
			continue
		}
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
	profilingConfig := m.getProfilingConfig(component)
	cfg := config.GetGlobalConfig()
	httpCfg := cfg.Security.GetHTTPClientConfig()
	addr := fmt.Sprintf("%v:%v", component.IP, component.StatusPort)
	for profileName, profileConfig := range profilingConfig.PprofConfig {
		target := NewTarget(component.Name, addr, profileName, cfg.GetHTTPScheme(), profileConfig)
		client, err := commonconfig.NewClientFromConfig(httpCfg, component.Name)
		if err != nil {
			return err
		}
		scrape := newScraper(target, client)
		scrapeSuite := newScrapeSuite(ctx, scrape, m.store)
		pt := meta.ProfileTarget{
			Kind:      profileName,
			Component: component.Name,
			Address:   addr,
		}

		interval := time.Duration(continueProfilingCfg.IntervalSeconds) * time.Second
		timeout := time.Duration(continueProfilingCfg.TimeoutSeconds) * time.Second
		m.wg.Add(1)
		go utils.GoWithRecovery(func() {
			defer m.wg.Done()
			scrapeSuite.run(interval, timeout)
		}, nil)
		m.addScrapeSuite(pt, scrapeSuite)
	}
	m.curComponents[component] = struct{}{}
	log.Info("start component scrape",
		zap.String("component", component.Name),
		zap.String("address", addr))
	return nil
}

func (m *Manager) stopScrape(component topology.Component) {
	delete(m.curComponents, component)
	addr := fmt.Sprintf("%v:%v", component.IP, component.StatusPort)
	log.Info("stop component scrape",
		zap.String("component", component.Name),
		zap.String("address", addr))
	profilingConfig := m.getProfilingConfig(component)
	for profileName := range profilingConfig.PprofConfig {
		key := meta.ProfileTarget{
			Kind:      profileName,
			Component: component.Name,
			Address:   addr,
		}
		suite := m.deleteScrapeSuite(key)
		if suite == nil {
			continue
		}
		suite.stop()
	}
}

func (m *Manager) getProfilingConfig(component topology.Component) *config.ProfilingConfig {
	switch component.Name {
	case topology.ComponentTiDB, topology.ComponentPD:
		return goAppProfilingConfig()
	default:
		return nonGoAppProfilingConfig()
	}
}

func (m *Manager) addScrapeSuite(pt meta.ProfileTarget, suite *ScrapeSuite) {
	m.mu.Lock()
	m.scrapeSuites[pt] = suite
	m.mu.Unlock()
}

func (m *Manager) deleteScrapeSuite(pt meta.ProfileTarget) *ScrapeSuite {
	m.mu.Lock()
	suite := m.scrapeSuites[pt]
	if suite != nil {
		delete(m.scrapeSuites, pt)
	}
	m.mu.Unlock()
	return suite
}

func (m *Manager) GetAllCurrentScrapeSuite() ([]meta.ProfileTarget, []*ScrapeSuite) {
	m.mu.Lock()
	defer m.mu.Unlock()
	targets := make([]meta.ProfileTarget, 0, len(m.scrapeSuites))
	suites := make([]*ScrapeSuite, 0, len(m.scrapeSuites))
	for target, suite := range m.scrapeSuites {
		targets = append(targets, target)
		suites = append(suites, suite)
	}
	return targets, suites
}

func (m *Manager) Close() error {
	if m.cancel != nil {
		m.cancel()
	}
	err := m.store.Close()
	if err != nil {
		return err
	}
	m.wg.Wait()
	return nil
}

func goAppProfilingConfig() *config.ProfilingConfig {
	cfg := config.GetGlobalConfig().ContinueProfiling
	return &config.ProfilingConfig{
		PprofConfig: config.PprofConfig{
			"allocs": &config.PprofProfilingConfig{
				Path: "/debug/pprof/allocs",
			},
			"goroutine": &config.PprofProfilingConfig{
				Path:   "/debug/pprof/goroutine",
				Params: map[string]string{"debug": "2"},
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

func nonGoAppProfilingConfig() *config.ProfilingConfig {
	cfg := config.GetGlobalConfig().ContinueProfiling
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
