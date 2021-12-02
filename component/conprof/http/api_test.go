package http

import (
	"github.com/genjidb/genji"
	"github.com/gin-gonic/gin"
	"github.com/pingcap/ng_monitoring/component/conprof"
	"github.com/pingcap/ng_monitoring/component/topology"
	"github.com/pingcap/ng_monitoring/config"
	"github.com/pingcap/ng_monitoring/utils/testutil"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"testing"
)

type testSuite struct {
	tmpDir string
	db     *genji.DB
}

func (ts *testSuite) setup(t *testing.T) {
	var err error
	ts.tmpDir, err = ioutil.TempDir(os.TempDir(), "ngm-test-.*")
	require.NoError(t, err)
	ts.db = testutil.NewGenjiDB(t, ts.tmpDir)
	cfg := config.GetDefaultConfig()
	cfg.ContinueProfiling.Enable = true
	cfg.ContinueProfiling.ProfileSeconds = 1
	cfg.ContinueProfiling.IntervalSeconds = 1
	config.StoreGlobalConfig(&cfg)
}

func (ts *testSuite) close(t *testing.T) {
	err := ts.db.Close()
	err = os.RemoveAll(ts.tmpDir)
	require.NoError(t, err)
}

func TestAPI(t *testing.T) {
	ts := testSuite{}
	ts.setup(t)
	defer ts.close(t)

	topoSubScribe := make(topology.Subscriber)
	err := conprof.Init(ts.db, topoSubScribe)
	require.NoError(t, err)
	defer conprof.Stop()

	httpAddr := setupHTTPService(t)

	mockServer := testutil.CreateMockProfileServer(t)
	defer mockServer.Stop(t)

	addr := mockServer.Addr
	port := mockServer.Port
	components := []topology.Component{
		{Name: topology.ComponentPD, IP: addr, Port: port, StatusPort: port},
		{Name: topology.ComponentTiDB, IP: addr, Port: port, StatusPort: port},
		{Name: topology.ComponentTiKV, IP: addr, Port: port, StatusPort: port},
		{Name: topology.ComponentTiFlash, IP: addr, Port: port, StatusPort: port},
	}
	// notify topology
	topoSubScribe <- components

	// wait for scrape finish
	//time.Sleep(time.Millisecond * 1500)

	// test for group_profiles api
	resp, err := http.Get("http://" + httpAddr + "/continuous_profiling/group_profiles")
	require.NoError(t, err)
	require.Equal(t, 503, resp.StatusCode)
	body, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, `{"message":"need param begin_time","status":"error"}`, string(body))
}

func setupHTTPService(t *testing.T) string {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	gin.SetMode(gin.ReleaseMode)
	ng := gin.New()

	ng.Use(gin.Recovery())

	continuousProfilingGroup := ng.Group("/continuous_profiling")
	HTTPService(continuousProfilingGroup)
	httpServer := &http.Server{Handler: ng}

	go func() {
		if err = httpServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			require.NoError(t, err)
		}
	}()
	return listener.Addr().String()
}
