package topology

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ng-monitoring/component/domain"
	"github.com/pingcap/ng-monitoring/utils"
	"github.com/pingcap/tidb-dashboard/util/topo"
	"go.uber.org/zap"
)

const (
	ComponentTiDB    = "tidb"
	ComponentTiKV    = "tikv"
	ComponentTiFlash = "tiflash"
	ComponentPD      = "pd"
)

var (
	discoverInterval = time.Second * 30
)

type TopologyDiscoverer struct {
	sync.Mutex
	do         *domain.Domain
	subscriber []Subscriber
	components atomic.Value
	closed     chan struct{}
}

type Component struct {
	Name       string `json:"name"`
	IP         string `json:"ip"`
	Port       uint   `json:"port"`
	StatusPort uint   `json:"status_port"`
}

type Subscriber = chan GetLatestTopology
type GetLatestTopology = func() []Component

func NewTopologyDiscoverer(do *domain.Domain) (*TopologyDiscoverer, error) {
	d := &TopologyDiscoverer{
		do:     do,
		closed: make(chan struct{}),
	}
	return d, nil
}

func (d *TopologyDiscoverer) Subscribe() Subscriber {
	ch := make(Subscriber, 1)
	d.Lock()
	d.subscriber = append(d.subscriber, ch)
	ch <- d.load
	d.Unlock()
	return ch
}

func (d *TopologyDiscoverer) Start() {
	go utils.GoWithRecovery(d.loadTopologyLoop, nil)
}

func (d *TopologyDiscoverer) Close() error {
	close(d.closed)
	return nil
}

func (d *TopologyDiscoverer) loadTopologyLoop() {
	err := d.fetchTopology()
	log.Info("first load topology", zap.Reflect("component", d.components), zap.Error(err))
	ticker := time.NewTicker(discoverInterval)
	defer ticker.Stop()
	for {
		select {
		case <-d.closed:
			return
		case <-ticker.C:
			err = d.fetchTopology()
			if err != nil {
				log.Error("load topology failed", zap.Error(err))
			} else {
				log.Debug("load topology success", zap.Reflect("component", d.components))
			}
			d.notifySubscriber()
		}
	}
}

func (d *TopologyDiscoverer) fetchTopology() error {
	ctx, cancel := context.WithTimeout(context.Background(), discoverInterval)
	defer cancel()
	components, err := d.fetchAllScrapeTargets(ctx)
	if err != nil {
		return err
	}
	d.components.Store(components)
	return nil
}

func (d *TopologyDiscoverer) load() []Component {
	v := d.components.Load()
	if v == nil {
		return nil
	}
	return d.components.Load().([]Component)
}

func (d *TopologyDiscoverer) notifySubscriber() {
	d.Lock()
	for _, ch := range d.subscriber {
		select {
		case ch <- d.load:
		default:
		}
	}
	d.Unlock()
}

func (d *TopologyDiscoverer) fetchAllScrapeTargets(ctx context.Context) ([]Component, error) {
	fns := []func(context.Context) ([]Component, error){
		d.getTiDBComponents,
		d.getPDComponents,
		d.getStoreComponents,
	}
	components := make([]Component, 0, 8)
	for _, fn := range fns {
		nodes, err := fn(ctx)
		if err != nil {
			return nil, err
		}
		components = append(components, nodes...)
	}
	return components, nil
}

func (d *TopologyDiscoverer) getTiDBComponents(ctx context.Context) ([]Component, error) {
	etcdCli, err := d.do.GetEtcdClient()
	if err != nil {
		return nil, err
	}
	instances, err := topo.GetTiDBInstances(ctx, etcdCli)
	if err != nil {
		return nil, err
	}
	components := make([]Component, 0, len(instances))
	for _, instance := range instances {
		if instance.Status != topo.ComponentStatusUp {
			continue
		}
		components = append(components, Component{
			Name:       ComponentTiDB,
			IP:         instance.IP,
			Port:       instance.Port,
			StatusPort: instance.StatusPort,
		})
	}
	return components, nil
}

func (d *TopologyDiscoverer) getPDComponents(ctx context.Context) ([]Component, error) {
	pdCli, err := d.do.GetPDClient()
	if err != nil {
		return nil, err
	}
	instances, err := topo.GetPDInstances(pdCli)
	if err != nil {
		return nil, err
	}
	components := make([]Component, 0, len(instances))
	for _, instance := range instances {
		if instance.Status != topo.ComponentStatusUp {
			continue
		}
		components = append(components, Component{
			Name:       ComponentPD,
			IP:         instance.IP,
			Port:       instance.Port,
			StatusPort: instance.Port,
		})
	}
	return components, nil
}

func (d *TopologyDiscoverer) getStoreComponents(ctx context.Context) ([]Component, error) {
	pdCli, err := d.do.GetPDClient()
	if err != nil {
		return nil, err
	}
	tikvInstances, tiflashInstances, err := topo.GetStoreInstances(pdCli)
	if err != nil {
		return nil, err
	}
	components := make([]Component, 0, len(tikvInstances)+len(tiflashInstances))
	getComponents := func(instances []topo.StoreInfo, name string) {
		for _, instance := range instances {
			if instance.Status != topo.ComponentStatusUp {
				continue
			}
			components = append(components, Component{
				Name:       name,
				IP:         instance.IP,
				Port:       instance.Port,
				StatusPort: instance.StatusPort,
			})
		}
	}
	getComponents(tikvInstances, ComponentTiKV)
	getComponents(tiflashInstances, ComponentTiFlash)
	return components, nil
}
