package topology

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb-dashboard/util/topo"
	"github.com/zhongzc/ng_monitoring/config"
	"github.com/zhongzc/ng_monitoring/utils"
	"go.uber.org/zap"
)

const (
	discoverInterval = time.Second * 30
	ComponentTiDB    = "tidb"
	ComponentTiKV    = "tikv"
	ComponentTiFlash = "tiflash"
	ComponentPD      = "pd"
)

type TopologyDiscoverer struct {
	sync.Mutex
	cli         *Client
	subscriber  []chan []Component
	components  []Component
	notifyCh    chan struct{}
	cfgChangeCh chan struct{}
	closed      chan struct{}
}

type Component struct {
	Name       string `json:"name"`
	IP         string `json:"ip"`
	Port       uint   `json:"port"`
	StatusPort uint   `json:"status_port"`
}

type Subscriber = chan []Component

func NewTopologyDiscoverer(cfg *config.Config, configChangeCh chan struct{}) (*TopologyDiscoverer, error) {
	cli, err := NewClient(cfg)
	if err != nil {
		return nil, err
	}
	d := &TopologyDiscoverer{
		cli:         cli,
		cfgChangeCh: configChangeCh,
		notifyCh:    make(chan struct{}, 1),
		closed:      make(chan struct{}),
	}
	return d, nil
}

func (d *TopologyDiscoverer) Subscribe() chan []Component {
	ch := make(chan []Component, 1)
	d.Lock()
	d.subscriber = append(d.subscriber, ch)
	d.Unlock()

	select {
	case d.notifyCh <- struct{}{}:
	default:
	}
	return ch
}

func (d *TopologyDiscoverer) Start() {
	go utils.GoWithRecovery(d.loadTopologyLoop, nil)
}

func (d *TopologyDiscoverer) Close() error {
	close(d.closed)
	return d.cli.Close()
}

func (d *TopologyDiscoverer) loadTopologyLoop() {
	err := d.loadTopology()
	log.Info("first load topology", zap.Reflect("component", d.components), zap.Error(err))
	ticker := time.NewTicker(discoverInterval)
	defer ticker.Stop()
	for {
		select {
		case <-d.closed:
			return
		case <-d.cfgChangeCh:
			newCfg := config.GetGlobalConfig()
			d.cli.reCreateClient(newCfg)
		case <-ticker.C:
			err = d.loadTopology()
			if err != nil {
				log.Error("load topology failed", zap.Error(err))
			} else {
				log.Debug("load topology success", zap.Reflect("component", d.components))
			}
			d.notifySubscriber()
		case <-d.notifyCh:
			d.notifySubscriber()
		}
	}
}

func (d *TopologyDiscoverer) loadTopology() error {
	ctx, cancel := context.WithTimeout(context.Background(), discoverInterval)
	defer cancel()
	components, err := d.getAllScrapeTargets(ctx)
	if err != nil {
		return err
	}
	d.components = components
	return nil
}

func (d *TopologyDiscoverer) notifySubscriber() {
	for _, ch := range d.subscriber {
		select {
		case ch <- d.components:
		default:
		}
	}
}

func (d *TopologyDiscoverer) getAllScrapeTargets(ctx context.Context) ([]Component, error) {
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
	instances, err := topo.GetTiDBInstances(ctx, d.cli.etcdCli)
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
	instances, err := topo.GetPDInstances(d.cli.pdCli)
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
	tikvInstances, tiflashInstances, err := topo.GetStoreInstances(d.cli.pdCli)
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
