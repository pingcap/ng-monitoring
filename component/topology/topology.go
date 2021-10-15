package topology

import (
	"github.com/zhongzc/ng_monitoring/config"
)

var (
	discover *TopologyDiscoverer
	syncer   *TopologySyncer
)

func Init() error {
	var err error
	cfg := config.GetGlobalConfig()
	// TODO: support all PD endpoints.
	discover, err = NewTopologyDiscoverer(cfg.PD.Endpoints[0], cfg.Security.GetTLSConfig())
	if err != nil {
		return err
	}
	syncer, err = NewTopologySyncer(discover.etcdCli)
	if err != nil {
		return err
	}
	discover.Start()
	return err
}

func Subscribe() Subscriber {
	return discover.Subscribe()
}

func Stop() {
	syncer.Stop()
}
