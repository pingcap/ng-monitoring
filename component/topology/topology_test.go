package topology

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/pingcap/ng-monitoring/component/domain"
	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/ng-monitoring/utils/testutil"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/integration"
)

func TestBasic(t *testing.T) {
	cfg := config.GetDefaultConfig()
	config.StoreGlobalConfig(&cfg)
	do := domain.NewDomain()
	defer do.Close()
	err := Init(do)
	require.NoError(t, err)
	comps := GetCurrentComponent()
	require.Equal(t, len(comps), 0)

	InitForTest([]Component{
		{Name: ComponentTiDB},
	})
	comps = GetCurrentComponent()
	require.Equal(t, len(comps), 1)
	require.Equal(t, comps[0].Name, ComponentTiDB)
}

func TestTopology(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create file contains a colon which is not allowed on Windows")
	}
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer cluster.Terminate(t)
	mockPD := testutil.MockPDHTTPServer{}
	mockPD.Setup(t)
	defer mockPD.Close(t)

	cfg := config.GetDefaultConfig()
	cfg.AdvertiseAddress = "10.0.1.8:12020"
	_, err := domain.CreatePDClient(&cfg)
	require.NotNil(t, err)
	require.Equal(t, "need specify pd endpoints", err.Error())
	cfg.PD.Endpoints = []string{mockPD.Addr}
	config.StoreGlobalConfig(&cfg)
	mockPD.Health = false
	_, err = domain.CreatePDClient(&cfg)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "Response status 503")
	mockPD.Health = true
	pdCli, err := domain.CreatePDClient(&cfg)
	require.NoError(t, err)

	do := domain.NewDomainForTest(pdCli, cluster.RandClient())
	discover = &TopologyDiscoverer{
		do: do,

		notifyCh: make(chan struct{}, 1),
		closed:   make(chan struct{}),
	}
	err = discover.loadTopology()
	require.NoError(t, err)

	sub := discover.Subscribe()
	discoverInterval = time.Millisecond * 100
	go discover.loadTopologyLoop()

	// test for config change channel
	//discover.cfgChangeCh <- struct{}{}

	components := <-sub
	require.Equal(t, len(components), 2)
	require.Equal(t, components[0].Name, "pd")
	require.Equal(t, components[1].Name, "tikv")

	// test syncer
	syncer = NewTopologySyncer(do)
	err = syncer.newTopologySessionAndStoreServerInfo()
	require.NoError(t, err)
	err = syncer.storeTopologyInfo()
	require.NoError(t, err)

	// get ngm topology from etcd.
	etcdCli := cluster.RandClient()
	resp, err := etcdCli.Get(context.Background(), topologyPrefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, 1, int(resp.Count))
	require.Equal(t, "/topology/ng-monitoring/10.0.1.8:12020/ttl", string(resp.Kvs[0].Key))

	// test syncer sync.
	topologyTimeToRefresh = time.Millisecond * 10
	syncer.Start()
	respd, err := etcdCli.Delete(context.Background(), topologyPrefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, 1, int(respd.Deleted))
	// wait syncer to restore the info.
	time.Sleep(time.Millisecond * 100)
	resp, err = etcdCli.Get(context.Background(), topologyPrefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, 1, int(resp.Count))
	require.Equal(t, "/topology/ng-monitoring/10.0.1.8:12020/ttl", string(resp.Kvs[0].Key))
	Stop()
}
