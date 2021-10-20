package topology

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/log"
	dashboard_config "github.com/pingcap/tidb-dashboard/pkg/config"
	"github.com/pingcap/tidb-dashboard/pkg/httpc"
	"github.com/pingcap/tidb-dashboard/pkg/pd"
	"github.com/pingcap/tidb-dashboard/pkg/utils/topology"
	"github.com/zhongzc/ng_monitoring/utils"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/fx"
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
	pdCli      *pd.Client
	etcdCli    *clientv3.Client
	subscriber []chan []Component
	components []Component
	notifyCh   chan struct{}
	closed     chan struct{}
}

type Component struct {
	Name       string `json:"name"`
	IP         string `json:"ip"`
	Port       uint   `json:"port"`
	StatusPort uint   `json:"status_port"`
}

type Subscriber = chan []Component

func NewTopologyDiscoverer(pdAddr string, tlsConfig *tls.Config) (*TopologyDiscoverer, error) {
	cfg := buildDashboardConfig(pdAddr, tlsConfig)
	lc := &mockLifecycle{}
	httpCli := httpc.NewHTTPClient(lc, cfg)
	pdCli := pd.NewPDClient(lc, httpCli, cfg)
	etcdCli, err := pd.NewEtcdClient(lc, cfg)
	if err != nil {
		return nil, err
	}
	d := &TopologyDiscoverer{
		pdCli:    pdCli,
		etcdCli:  etcdCli,
		notifyCh: make(chan struct{}, 1),
		closed:   make(chan struct{}),
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
	return d.etcdCli.Close()
}

func (d *TopologyDiscoverer) loadTopologyLoop() {
	d.loadTopology()
	ticker := time.NewTicker(discoverInterval)
	defer ticker.Stop()
	for {
		select {
		case <-d.closed:
			return
		case <-ticker.C:
			d.loadTopology()
			d.notifySubscriber()
		case <-d.notifyCh:
			d.notifySubscriber()
		}
	}
}

func (d *TopologyDiscoverer) loadTopology() {
	ctx, cancel := context.WithTimeout(context.Background(), discoverInterval)
	defer cancel()
	components, err := d.getAllScrapeTargets(ctx)
	if err != nil {
		log.Error("load topology failed", zap.Error(err))
		return
	}
	d.components = components
	log.Debug("load topology success", zap.Reflect("component", components))
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
	instances, err := topology.FetchTiDBTopology(ctx, d.etcdCli)
	if err != nil {
		return nil, err
	}
	components := make([]Component, 0, len(instances))
	for _, instance := range instances {
		if instance.Status != topology.ComponentStatusUp {
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
	instances, err := topology.FetchPDTopology(d.pdCli)
	if err != nil {
		return nil, err
	}
	components := make([]Component, 0, len(instances))
	for _, instance := range instances {
		if instance.Status != topology.ComponentStatusUp {
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
	tikvInstances, tiflashInstances, err := topology.FetchStoreTopology(d.pdCli)
	if err != nil {
		return nil, err
	}
	components := make([]Component, 0, len(tikvInstances)+len(tiflashInstances))
	getComponents := func(instances []topology.StoreInfo, name string) {
		for _, instance := range instances {
			if instance.Status != topology.ComponentStatusUp {
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

func buildDashboardConfig(pdAddr string, tlsConfig *tls.Config) *dashboard_config.Config {
	schema := "http"
	if tlsConfig != nil {
		schema = "https"
	}
	return &dashboard_config.Config{
		PDEndPoint:       fmt.Sprintf("%v://%v", schema, pdAddr),
		ClusterTLSConfig: tlsConfig,
	}
}

type mockLifecycle struct{}

func (_ *mockLifecycle) Append(fx.Hook) {
	return
}
