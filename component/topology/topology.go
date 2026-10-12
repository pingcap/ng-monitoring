package topology

import (
	"github.com/pingcap/ng-monitoring/component/domain"
)

var (
	discover *TopologyDiscoverer
	syncer   *TopologySyncer
)

func Init(do *domain.Domain) error {
	var err error
	discover, err = NewTopologyDiscoverer(do)
	if err != nil {
		return err
	}
	syncer, err = NewTopologySyncer(do)
	if err != nil {
		return err
	}
	syncer.Start()
	discover.Start()
	return err
}

func GetDashboardAuthToken() string {
	if syncer == nil || syncer.serverInfo == nil || syncer.serverInfo.DashboardAuth == nil {
		return ""
	}
	return syncer.serverInfo.DashboardAuth.Token
}

func InitForTest(comps []Component) {
	discover = &TopologyDiscoverer{}
	discover.components.Store(comps)
}

func GetCurrentComponent() []Component {
	if discover == nil {
		return nil
	}
	return discover.load()
}

func Subscribe() Subscriber {
	return discover.Subscribe()
}

func Stop() {
	if syncer != nil {
		syncer.Stop()
	}
	if discover != nil {
		_ = discover.Close()
	}
}
