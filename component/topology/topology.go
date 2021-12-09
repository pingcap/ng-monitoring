package topology

import (
	"github.com/pingcap/ng_monitoring/component/domain"
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
	syncer = NewTopologySyncer(do)
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

func Subscribe() Subscriber {
	return discover.Subscribe()
}

func Stop() {
	if syncer == nil {
		return
	}
	syncer.Stop()
}
