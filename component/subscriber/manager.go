package subscriber

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/ng-monitoring/component/domain"
	"github.com/pingcap/ng-monitoring/component/subscriber/model"
	"github.com/pingcap/ng-monitoring/component/topology"
	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/ng-monitoring/config/pdvariable"
	"github.com/pingcap/ng-monitoring/utils"
	"go.uber.org/zap"

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
	httpCli        *http.Client

	do            *domain.Domain
	schemaCache   *sync.Map
	schemaVersion int64

	subscribeController SubscribeController
}

func NewManager(
	ctx context.Context,
	wg *sync.WaitGroup,
	do *domain.Domain,
	varSubscriber pdvariable.Subscriber,
	topoSubscriber topology.Subscriber,
	cfgSubscriber config.Subscriber,
	subscribeController SubscribeController,
) *Manager {
	return &Manager{
		ctx: ctx,
		wg:  wg,

		scrapers:    make(map[topology.Component]Scraper),
		schemaCache: &sync.Map{},

		varSubscriber:  varSubscriber,
		topoSubscriber: topoSubscriber,
		cfgSubscriber:  cfgSubscriber,

		do:                  do,
		prevEnabled:         subscribeController.IsEnabled(),
		subscribeController: subscribeController,
	}
}

func (m *Manager) Run() {
	defer m.clearScrapers()
	ticker := time.NewTicker(30 * time.Second)
	for {
		select {
		case getCfg := <-m.cfgSubscriber:
			m.subscribeController.UpdateConfig(getCfg())
			m.httpCli = m.subscribeController.NewHTTPClient()
		case getVars := <-m.varSubscriber:
			m.subscribeController.UpdatePDVariable(getVars())
		case getTopology := <-m.topoSubscriber:
			m.components = getTopology()
			m.subscribeController.UpdateTopology(getTopology())
		case <-ticker.C:
			m.updateSchemaCache()
			continue
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

func (m *Manager) updateSchemaCache() {
	if !m.subscribeController.IsEnabled() {
		// clear cache
		m.schemaCache.Range(func(k, v interface{}) bool {
			m.schemaCache.Delete(k)
			return true
		})
		m.schemaVersion = 0
		return
	}
	if m.do == nil {
		return
	}

	ectx, cancel := context.WithTimeout(context.TODO(), 3*time.Second)
	etcdCli, err := m.do.GetEtcdClient()
	defer cancel()
	if err != nil {
		log.Error("failed to get etcd client", zap.Error(err))
		return
	}
	resp, err := etcdCli.Get(ectx, model.SchemaVersionPath)
	if err != nil || len(resp.Kvs) != 1 {
		if resp != nil && len(resp.Kvs) == 0 {
			return
		}
		log.Warn("failed to get tidb schema version", zap.Error(err))
		return
	}
	schemaVersion, err := strconv.ParseInt(string(resp.Kvs[0].Value), 10, 64)
	if err != nil {
		log.Warn("failed to get tidb schema version", zap.Error(err))
		return
	}
	if schemaVersion == m.schemaVersion {
		return
	}
	log.Info("schema version changed", zap.Int64("old", m.schemaVersion), zap.Int64("new", schemaVersion))
	m.tryUpdateSchemaCache(schemaVersion)
}

type getConfig interface {
	GetConfig() *config.Config
}

func (m *Manager) requestDB(path string, v interface{}) error {
	schema := "http"
	if sc, ok := m.subscribeController.(getConfig); ok && sc.GetConfig().Security.GetTLSConfig() != nil {
		schema = "https"
	}
	for _, compt := range m.components {
		if compt.Name != topology.ComponentTiDB {
			continue
		}

		url := fmt.Sprintf("%s://%s:%d%s", schema, compt.IP, compt.StatusPort, path)
		resp, err := m.httpCli.Get(url)
		if err != nil {
			log.Error("request failed", zap.Error(err))
			continue
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			log.Error("request failed", zap.String("status", resp.Status))
			continue
		}
		if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
			log.Error("decode response failed", zap.Error(err))
			continue
		}
		return nil
	}
	return fmt.Errorf("all request failed")
}

func (m *Manager) tryUpdateSchemaCache(schemaVersion int64) {
	// get all database info
	var dbInfos []*model.DBInfo
	if err := m.requestDB("/schema", &dbInfos); err != nil {
		return
	}

	// get all table info
	updateSuccess := true
	for _, db := range dbInfos {
		if db.State == model.StateNone {
			continue
		}
		var tableInfos []*model.TableInfo
		encodeName := url.PathEscape(db.Name.O)
		if err := m.requestDB(fmt.Sprintf("/schema/%s?id_name_only=true", encodeName), &tableInfos); err != nil {
			updateSuccess = false
			continue
		}
		log.Info("update table info", zap.String("db", db.Name.O), zap.Reflect("table-info", tableInfos))
		if len(tableInfos) == 0 {
			continue
		}
		for _, table := range tableInfos {
			indices := make(map[int64]string, len(table.Indices))
			for _, index := range table.Indices {
				indices[index.ID] = index.Name.O
			}
			detail := &model.TableDetail{
				Name:    table.Name.O,
				DB:      db.Name.O,
				ID:      table.ID,
				Indices: indices,
			}
			m.schemaCache.Store(table.ID, detail)
			if partition := table.GetPartitionInfo(); partition != nil {
				for _, partitionDef := range partition.Definitions {
					detail := &model.TableDetail{
						Name:    fmt.Sprintf("%s/%s", table.Name.O, partitionDef.Name.O),
						DB:      db.Name.O,
						ID:      partitionDef.ID,
						Indices: indices,
					}
					m.schemaCache.Store(partitionDef.ID, detail)
				}
			}
		}
	}
	if updateSuccess {
		m.schemaVersion = schemaVersion
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
		scraper := m.subscribeController.NewScraper(m.ctx, in[i], m.schemaCache)
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
