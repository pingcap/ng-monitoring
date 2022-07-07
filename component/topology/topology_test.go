package topology

import (
	"context"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/ng-monitoring/component/domain"
	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/ng-monitoring/utils/printer"
	"github.com/pingcap/ng-monitoring/utils/testutil"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
)

func TestBasic(t *testing.T) {
	cfg := config.GetDefaultConfig()
	config.StoreGlobalConfig(cfg)
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

	integration.BeforeTestExternal(t)
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer cluster.Terminate(t)
	mockPD := testutil.MockPDHTTPServer{}
	mockPD.Setup(t)
	defer mockPD.Close(t)

	cfg := config.GetDefaultConfig()
	cfg.PD.Endpoints = nil
	cfg.AdvertiseAddress = "10.0.1.8:12020"
	_, err := domain.CreatePDClient(&cfg)
	require.NotNil(t, err)
	require.Equal(t, "need specify pd endpoints", err.Error())
	cfg.PD.Endpoints = []string{mockPD.Addr}
	config.StoreGlobalConfig(cfg)
	mockPD.Health = false
	_, err = domain.CreatePDClient(&cfg)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "Response status 503")
	mockPD.Health = true
	pdCli, err := domain.CreatePDClient(&cfg)
	require.NoError(t, err)

	do := domain.NewDomainForTest(pdCli, cluster.RandClient())
	discover = &TopologyDiscoverer{
		do:     do,
		closed: make(chan struct{}),
	}
	err = discover.fetchTopology()
	require.NoError(t, err)

	sub := discover.Subscribe()
	discoverInterval = time.Millisecond * 100
	discover.Start()

	getComponents := <-sub
	components := getComponents()
	require.Equal(t, len(components), 2)
	require.Equal(t, components[0].Name, "pd")
	require.Equal(t, components[1].Name, "tikv")

	// test syncer
	printer.NGMGitHash = "b225682e6660cb617b8f4ccc77da252f845f411c"
	syncer = NewTopologySyncer(do)
	require.Equal(t, "10.0.1.8", syncer.serverInfo.IP)
	require.Equal(t, uint64(12020), syncer.serverInfo.Port)
	require.Equal(t, "b225682e6660cb617b8f4ccc77da252f845f411c", syncer.serverInfo.GitHash)
	require.True(t, syncer.serverInfo.StartTimestamp > 0)
	syncer.serverInfo.StartTimestamp = 1639643120
	err = syncer.newTopologySessionAndStoreServerInfo()
	require.NoError(t, err)
	err = syncer.storeTopologyInfo()
	require.NoError(t, err)

	etcdCli := cluster.RandClient()
	check := func() {
		// get ngm topology from etcd.
		resp, err := etcdCli.Get(context.Background(), "/topology/ng-monitoring/10.0.1.8:12020/ttl")
		require.NoError(t, err)
		require.Equal(t, 1, int(resp.Count))
		require.Equal(t, "/topology/ng-monitoring/10.0.1.8:12020/ttl", string(resp.Kvs[0].Key))
		ts, err := strconv.Atoi(string(resp.Kvs[0].Value))
		require.NoError(t, err)
		require.True(t, ts > 0)

		// get ngm server info from etcd.
		resp, err = etcdCli.Get(context.Background(), "/topology/ng-monitoring/10.0.1.8:12020/info")
		require.NoError(t, err)
		require.Equal(t, 1, int(resp.Count))
		require.Equal(t, "/topology/ng-monitoring/10.0.1.8:12020/info", string(resp.Kvs[0].Key))
		require.Equal(t, `{"git_hash":"b225682e6660cb617b8f4ccc77da252f845f411c","ip":"10.0.1.8","listening_port":12020,"start_timestamp":1639643120}`, string(resp.Kvs[0].Value))
	}
	check()

	// test syncer sync.
	topologyTimeToRefresh = time.Millisecond * 10
	syncer.Start()
	respd, err := etcdCli.Delete(context.Background(), topologyPrefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, 2, int(respd.Deleted))
	// wait syncer to restore the info.
	time.Sleep(time.Millisecond * 100)
	resp, err := etcdCli.Get(context.Background(), topologyPrefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, 2, int(resp.Count))
	check()
	Stop()

	// Test invalid address
	cfg.AdvertiseAddress = "abcd"
	config.StoreGlobalConfig(cfg)
	serverInfo := getServerInfo()
	require.Equal(t, "", serverInfo.IP)
	require.Equal(t, uint64(0), serverInfo.Port)
	cfg.AdvertiseAddress = "abcd:x"
	config.StoreGlobalConfig(cfg)
	serverInfo = getServerInfo()
	require.Equal(t, "abcd", serverInfo.IP)
	require.Equal(t, uint64(0), serverInfo.Port)
}
