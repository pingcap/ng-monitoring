package topology

import (
	"github.com/zhongzc/ng_monitoring/config"
	"go.etcd.io/etcd/clientv3"
)

var (
	discover *TopologyDiscoverer
	syncer   *TopologySyncer
)

func Init() error {
	var err error
	discover, err = NewTopologyDiscoverer(config.GetGlobalConfig())
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

func GetEtcdClient() *clientv3.Client {
	return discover.etcdCli
}

func Subscribe() Subscriber {
	return discover.Subscribe()
}

func Stop() {
	syncer.Stop()
}
