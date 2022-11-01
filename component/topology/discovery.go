package topology

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/ng-monitoring/component/domain"
	"github.com/pingcap/ng-monitoring/utils"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb-dashboard/util/topo"
	"github.com/pingcap/tidb-dashboard/util/topo/pdtopo"
	"go.uber.org/zap"
)

const (
	ComponentTiDB    = "tidb"
	ComponentTiKV    = "tikv"
	ComponentTiFlash = "tiflash"
	ComponentPD      = "pd"
	ComponentTiCDC   = "ticdc"
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
		d.getTiCDCComponents,
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
	instances, err := pdtopo.GetTiDBInstances(ctx, etcdCli)
	if err != nil {
		return nil, err
	}
	components := make([]Component, 0, len(instances))
	for _, instance := range instances {
		if instance.Status != topo.CompStatusUp {
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
	instances, err := pdtopo.GetPDInstances(ctx, pdCli)
	if err != nil {
		return nil, err
	}
	components := make([]Component, 0, len(instances))
	for _, instance := range instances {
		if instance.Status != topo.CompStatusUp {
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
	tikvInstances, tiflashInstances, err := pdtopo.GetStoreInstances(ctx, pdCli)
	if err != nil {
		return nil, err
	}
	components := make([]Component, 0, len(tikvInstances)+len(tiflashInstances))
	for _, instance := range tikvInstances {
		if instance.Status != topo.CompStatusUp {
			continue
		}
		components = append(components, Component{
			Name:       ComponentTiKV,
			IP:         instance.IP,
			Port:       instance.Port,
			StatusPort: instance.StatusPort,
		})
	}
	for _, instance := range tiflashInstances {
		if instance.Status != topo.CompStatusUp {
			continue
		}
		components = append(components, Component{
			Name:       ComponentTiFlash,
			IP:         instance.IP,
			Port:       instance.Port,
			StatusPort: instance.StatusPort,
		})
	}
	return components, nil
}

func (d *TopologyDiscoverer) getTiCDCComponents(ctx context.Context) ([]Component, error) {
	etcdCli, err := d.do.GetEtcdClient()
	if err != nil {
		return nil, err
	}
	return getTiCDCComponents(ctx, etcdCli)
}

const ticdcTopologyKeyPrefix = "/tidb/cdc/default/__cdc_meta__/capture/"

type ticdcNodeItem struct {
	ID      string `json:"id"`
	Address string `json:"address"`
	Version string `json:"version"`
}

func getTiCDCComponents(ctx context.Context, etcdCli *clientv3.Client) ([]Component, error) {
	resp, err := etcdCli.Get(ctx, ticdcTopologyKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	components := make([]Component, 0, 3)
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		if !strings.HasPrefix(key, ticdcTopologyKeyPrefix) {
			continue
		}
		var item ticdcNodeItem
		if err := json.Unmarshal(kv.Value, &item); err != nil {
			log.Warn("invalid ticdc node item in etcd", zap.Error(err))
			continue
		}
		arr := strings.Split(item.Address, ":")
		if len(arr) != 2 {
			log.Warn("invalid ticdc node address in etcd", zap.String("address", item.Address))
			continue
		}
		ip := arr[0]
		port, err := strconv.Atoi(arr[1])
		if err != nil {
			log.Warn("invalid ticdc node address in etcd",
				zap.Error(err),
				zap.String("address", item.Address))
			continue
		}
		components = append(components, Component{
			Name:       ComponentTiCDC,
			IP:         ip,
			Port:       uint(port),
			StatusPort: uint(port),
		})
	}
	return components, nil
}
