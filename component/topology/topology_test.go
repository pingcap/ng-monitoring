package topology

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"runtime"
	"testing"
	"time"

	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/tidb-dashboard/util/client/pdclient"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/integration"
)

func TestTopology(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create file contains a colon which is not allowed on Windows")
	}
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer cluster.Terminate(t)
	mockPD := mockPDHTTPServer{}
	mockPD.setup(t)
	defer mockPD.down(t)

	cfg := config.GetDefaultConfig()
	cfg.AdvertiseAddress = "10.0.1.8:12020"
	cli, err := NewClientForTest(&cfg, cluster.RandClient())
	require.NotNil(t, err)
	require.Equal(t, "need specify pd endpoints", err.Error())
	cfg.PD.Endpoints = []string{mockPD.addr}
	config.StoreGlobalConfig(&cfg)
	mockPD.health = false
	cli, err = NewClientForTest(&cfg, cluster.RandClient())
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "Response status 503")
	mockPD.health = true
	cli, err = NewClientForTest(&cfg, cluster.RandClient())
	require.NoError(t, err)

	discover = &TopologyDiscoverer{
		cli:         cli,
		cfgChangeCh: make(chan struct{}, 1),
		notifyCh:    make(chan struct{}, 1),
		closed:      make(chan struct{}),
	}
	err = discover.loadTopology()
	require.NoError(t, err)

	sub := discover.Subscribe()
	discoverInterval = time.Millisecond * 100
	go discover.loadTopologyLoop()

	// test for config change channel
	discover.cfgChangeCh <- struct{}{}

	components := <-sub
	require.Equal(t, len(components), 2)
	require.Equal(t, components[0].Name, "pd")
	require.Equal(t, components[1].Name, "tikv")

	// test syncer
	syncer = NewTopologySyncer()
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

type mockPDHTTPServer struct {
	addr       string
	httpServer *http.Server
	health     bool
}

func (s *mockPDHTTPServer) setup(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	router := http.NewServeMux()
	router.HandleFunc("/pd/api/v1/health", func(writer http.ResponseWriter, request *http.Request) {
		if !s.health {
			writer.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		resp := pdclient.GetHealthResponse{
			{MemberID: 1, Health: s.health},
		}
		s.writeJson(t, writer, resp)
	})

	router.HandleFunc("/pd/api/v1/status", func(writer http.ResponseWriter, request *http.Request) {
		resp := pdclient.GetStatusResponse{
			StartTimestamp: time.Now().Unix(),
		}
		s.writeJson(t, writer, resp)
	})

	router.HandleFunc("/pd/api/v1/members", func(writer http.ResponseWriter, request *http.Request) {
		resp := pdclient.GetMembersResponse{
			Members: []struct {
				GitHash       string   `json:"git_hash"`
				ClientUrls    []string `json:"client_urls"`
				DeployPath    string   `json:"deploy_path"`
				BinaryVersion string   `json:"binary_version"`
				MemberID      uint64   `json:"member_id"`
			}{
				{GitHash: "abcd", ClientUrls: []string{"http://" + s.addr}, DeployPath: "data", BinaryVersion: "v5.3.0", MemberID: 1},
			},
		}
		s.writeJson(t, writer, resp)
	})

	router.HandleFunc("/pd/api/v1/stores", func(writer http.ResponseWriter, request *http.Request) {
		resp := pdclient.GetStoresResponse{
			Stores: []struct {
				Store pdclient.GetStoresResponseStore
			}{
				{pdclient.GetStoresResponseStore{Address: "127.0.0.1:20160", ID: 1, Version: "v5.3.0", StatusAddress: "127.0.0.1:20180", StartTimestamp: time.Now().Unix(), StateName: "up"}},
			},
		}
		s.writeJson(t, writer, resp)
	})

	httpServer := &http.Server{
		Handler: router,
	}
	go func() {
		if err = httpServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			require.NoError(t, err)
		}
	}()
	s.health = true
	s.addr = listener.Addr().String()
	s.httpServer = httpServer
}

func (s *mockPDHTTPServer) writeJson(t *testing.T, writer http.ResponseWriter, resp interface{}) {
	writer.WriteHeader(http.StatusOK)
	data, err := json.Marshal(resp)
	require.NoError(t, err)
	_, err = writer.Write(data)
	require.NoError(t, err)
}

func (s *mockPDHTTPServer) down(t *testing.T) {
	err := s.httpServer.Close()
	require.NoError(t, err)
}
