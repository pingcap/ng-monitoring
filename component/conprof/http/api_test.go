package http

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/genjidb/genji"
	"github.com/gin-gonic/gin"
	"github.com/pingcap/ng-monitoring/component/conprof"
	"github.com/pingcap/ng-monitoring/component/topology"
	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/ng-monitoring/utils/testutil"
	"github.com/stretchr/testify/require"
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
	}
	topology.InitForTest(components)
	// notify topology
	topoSubScribe <- components

	testErrorRequest(t, httpAddr)

	// wait for scrape finish
	time.Sleep(time.Millisecond * 1500)

	validTs := testAPIGroupProfiles(t, httpAddr)
	testAPIGroupProfileDetail(t, httpAddr, validTs, components)
	testAPISingleProfileView(t, httpAddr, validTs, components)
	testAPIDownload(t, httpAddr, validTs, components)
	testAPIComponent(t, httpAddr, components)
	testAPIEstimateSize(t, httpAddr, components)
}

func testAPIGroupProfiles(t *testing.T, httpAddr string) int64 {
	ts := time.Now().Unix()
	resp, err := http.Get("http://" + httpAddr + "/continuous_profiling/group_profiles?begin_time=0&end_time=" + strconv.Itoa(int(ts)))
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)
	body, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)

	var groupProfiles []GroupProfiles
	err = json.Unmarshal(body, &groupProfiles)
	require.NoError(t, err)
	require.True(t, len(groupProfiles) > 0)
	for _, gp := range groupProfiles {
		require.Equal(t, ComponentNum{TiDB: 1, PD: 1, TiKV: 1, TiFlash: 0}, gp.CompNum)
		require.True(t, gp.Ts > 0)
	}
	return groupProfiles[0].Ts
}

func testAPIGroupProfileDetail(t *testing.T, httpAddr string, ts int64, components []topology.Component) {
	resp, err := http.Get("http://" + httpAddr + "/continuous_profiling/group_profile/detail?ts=" + strconv.Itoa(int(ts)))
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)
	body, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)

	var groupProfileDetail GroupProfileDetail
	err = json.Unmarshal(body, &groupProfileDetail)
	require.NoError(t, err)
	require.True(t, groupProfileDetail.Ts > 0)
	require.True(t, len(groupProfileDetail.TargetProfiles) >= len(components))
	for _, tp := range groupProfileDetail.TargetProfiles {
		require.Equal(t, "", tp.Error)
		require.Equal(t, "success", tp.State)
		found := false
		for _, comp := range components {
			if tp.Target.Component == comp.Name && tp.Target.Address == fmt.Sprintf("%v:%v", comp.IP, comp.Port) {
				found = true
				break
			}
		}
		require.True(t, found)
	}
}

func testAPISingleProfileView(t *testing.T, httpAddr string, ts int64, components []topology.Component) {
	for _, comp := range components {
		url := fmt.Sprintf("http://%v/continuous_profiling/single_profile/view?ts=%v&profile_type=profile&component=%v&address=%v:%v", httpAddr, ts, comp.Name, comp.IP, comp.Port)
		resp, err := http.Get(url)
		require.NoError(t, err)
		require.Equal(t, 200, resp.StatusCode)
		body, err := ioutil.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, "profile", string(body))
	}
}

func testAPIDownload(t *testing.T, httpAddr string, ts int64, components []topology.Component) {
	resp, err := http.Get("http://" + httpAddr + "/continuous_profiling/download?ts=" + strconv.Itoa(int(ts)))
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)
	body, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)

	zr, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err)
	require.True(t, len(zr.File) > len(components))
	for _, f := range zr.File {
		// file name format is: kind_component_ip_port_ts
		fields := strings.Split(f.Name, "_")
		require.True(t, len(fields) >= 4)
		reader, err := f.Open()
		require.NoError(t, err)

		// check content
		buf := make([]byte, len(fields[0]))
		n, _ := reader.Read(buf)
		require.Equal(t, len(fields[0]), n)
		require.Equal(t, fields[0], string(buf))

		found := false
		for _, comp := range components {
			if fields[1] == comp.Name && fields[2] == comp.IP {
				found = true
				break
			}
		}
		require.True(t, found, f.Name)
	}
}

func testAPIComponent(t *testing.T, httpAddr string, components []topology.Component) {
	resp, err := http.Get("http://" + httpAddr + "/continuous_profiling/components")
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)
	body, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)

	var comps []topology.Component
	err = json.Unmarshal(body, &comps)
	require.NoError(t, err)
	require.Equal(t, len(components), len(comps))
	for _, tc := range comps {
		found := false
		for _, comp := range components {
			if tc == comp {
				found = true
				break
			}
		}
		require.True(t, found, tc)
	}
}

func testAPIEstimateSize(t *testing.T, httpAddr string, components []topology.Component) {
	resp, err := http.Get("http://" + httpAddr + "/continuous_profiling/estimate_size")
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)
	body, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)

	var estimateSize EstimateSize
	err = json.Unmarshal(body, &estimateSize)
	require.NoError(t, err)
	require.Equal(t, len(components), estimateSize.InstanceCount, string(body))
	require.Equal(t, 88915968000, estimateSize.ProfileSize)
}

func testErrorRequest(t *testing.T, httpAddr string) {
	cases := []struct {
		api  string
		body string
	}{
		// test for group_profiles api
		{"/group_profiles", `{"message":"need param begin_time","status":"error"}`},
		{"/group_profiles?begin_time=0", `{"message":"need param end_time","status":"error"}`},
		{"/group_profiles?begin_time=0&end_time=zx", `{"message":"invalid param end_time value, error: strconv.ParseInt: parsing \"zx\": invalid syntax","status":"error"}`},

		// test for /group_profile/detail api.
		{"/group_profile/detail", `{"message":"need param ts","status":"error"}`},
		{"/group_profile/detail?ts=x", `{"message":"invalid param ts value, error: strconv.ParseInt: parsing \"x\": invalid syntax","status":"error"}`},
		{"/group_profile/detail?ts=0&limit=x", `{"message":"invalid param limit value, error: strconv.ParseInt: parsing \"x\": invalid syntax","status":"error"}`},

		// test for /single_profile/view api.
		{"/single_profile/view", `{"message":"need param ts","status":"error"}`},
		{"/single_profile/view?ts=x", `{"message":"invalid param ts value, error: strconv.ParseInt: parsing \"x\": invalid syntax","status":"error"}`},
		{"/single_profile/view?ts=0", `{"message":"need param profile_type","status":"error"}`},
		{"/single_profile/view?ts=0&profile_type=heap", `{"message":"need param component","status":"error"}`},
		{"/single_profile/view?ts=0&profile_type=heap&component=tidb", `{"message":"need param address","status":"error"}`},

		// test for /download api.
		{"/download", `{"message":"need param ts","status":"error"}`},
		{"/download?ts=x", `{"message":"invalid param ts value, error: strconv.ParseInt: parsing \"x\": invalid syntax","status":"error"}`},
	}

	for _, ca := range cases {
		resp, err := http.Get("http://" + httpAddr + "/continuous_profiling" + ca.api)
		require.NoError(t, err)
		require.Equal(t, 503, resp.StatusCode, ca.api)
		body, err := ioutil.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, ca.body, string(body), ca.api)
	}
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
