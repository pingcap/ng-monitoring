package continuousprofiling

import (
	"github.com/genjidb/genji"
	"github.com/zhongzc/ng_monitoring/component/continuousprofiling/scrape"
	"github.com/zhongzc/ng_monitoring/component/continuousprofiling/store"
	"github.com/zhongzc/ng_monitoring/component/topologydiscovery"
	"github.com/zhongzc/ng_monitoring/config"
)

var (
	storage  *store.ProfileStorage
	discover *topologydiscovery.TopologyDiscoverer
	manager  *scrape.Manager
)

func Init(db *genji.DB, cfg *config.Config) error {
	var err error
	storage, err = store.NewProfileStorage(db)
	if err != nil {
		return err
	}
	discover, err = topologydiscovery.NewTopologyDiscoverer(cfg.PD.Endpoints[0], cfg.Security.GetTLSConfig())
	if err != nil {
		return err
	}
	manager = scrape.NewManager(storage, discover.Subscribe())
	manager.Start()
	discover.Start()
	return nil
}

func Stop() {
	manager.Close()
}
