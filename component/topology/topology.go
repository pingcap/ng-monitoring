package topology

import (
	"github.com/pingcap/ng-monitoring/config"
	"go.etcd.io/etcd/clientv3"
)

var (
	discover *TopologyDiscoverer
	syncer   *TopologySyncer
)

func Init() error {
	var err error
	discover, err = NewTopologyDiscoverer(config.GetGlobalConfig(), config.SubscribeConfigChange())
	if err != nil {
		return err
	}
	syncer = NewTopologySyncer()
	syncer.Start()
	discover.Start()
	return err
}

func InitForTest(comps []Component) {
	discover = &TopologyDiscoverer{components: comps}
}

func GetCurrentComponent() []Component {
	if discover == nil {
		return nil
	}
	components := make([]Component, 0, len(discover.components))
	for _, comp := range discover.components {
		components = append(components, comp)
	}
	return components
}

func GetEtcdClient() *clientv3.Client {
	return discover.cli.etcdCli
}

func Subscribe() Subscriber {
	return discover.Subscribe()
}

func Stop() {
	if syncer == nil {
		return
	}
	syncer.Stop()
}
