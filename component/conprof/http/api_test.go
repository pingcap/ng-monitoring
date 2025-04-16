package http

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/ng-monitoring/component/conprof"
	"github.com/pingcap/ng-monitoring/component/conprof/meta"
	"github.com/pingcap/ng-monitoring/component/topology"
	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/ng-monitoring/database/docdb"
	"github.com/pingcap/ng-monitoring/utils/testutil"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type testSuite struct {
	tmpDir string
	db     docdb.DocDB
}

func (ts *testSuite) setup(t *testing.T) {
	var err error
	ts.tmpDir, err = os.MkdirTemp(os.TempDir(), "ngm-test-.*")
	require.NoError(t, err)
	ts.db, err = docdb.NewGenjiDBFromGenji(testutil.NewGenjiDB(t, ts.tmpDir))
	require.NoError(t, err)
	cfg := config.GetDefaultConfig()
	cfg.ContinueProfiling.Enable = true
	cfg.ContinueProfiling.ProfileSeconds = 1
	cfg.ContinueProfiling.IntervalSeconds = 1
	config.StoreGlobalConfig(cfg)
}

func (ts *testSuite) close(t *testing.T) {
	err := ts.db.Close()
	require.NoError(t, err)
	err = os.RemoveAll(ts.tmpDir)
	require.NoError(t, err)
}

func TestAPI(t *testing.T) {
	ts := testSuite{}
	ts.setup(t)
	defer ts.close(t)

	httpAddr := setupHTTPService(t)
	mockServer := testutil.CreateMockProfileServer(t)
	defer mockServer.Stop(t)

	// wait for http server ready
	time.Sleep(time.Second)

	topoSubScribe := make(topology.Subscriber)
	err := conprof.Init(ts.db, topoSubScribe)
	require.NoError(t, err)
	defer conprof.Stop()

	addr := mockServer.Addr
	port := mockServer.Port
	components := []topology.Component{
		{Name: topology.ComponentPD, IP: addr, Port: port, StatusPort: port},
		{Name: topology.ComponentTiDB, IP: addr, Port: port, StatusPort: port},
		{Name: topology.ComponentTiKV, IP: addr, Port: port, StatusPort: port},
	}
	topology.InitForTest(components)
	// notify topology
	topoSubScribe <- func() []topology.Component { return components }

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
	resp, err := http.Get(fmt.Sprintf("http://%v/continuous_profiling/group_profiles?begin_time=%v&end_time=%v", httpAddr, ts-2*60*60, ts))
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	err = resp.Body.Close()
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
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	err = resp.Body.Close()
	require.NoError(t, err)

	var groupProfileDetail GroupProfileDetail
	err = json.Unmarshal(body, &groupProfileDetail)
	require.NoError(t, err)
	require.True(t, groupProfileDetail.Ts > 0)
	require.True(t, len(groupProfileDetail.TargetProfiles) >= len(components))
	for _, tp := range groupProfileDetail.TargetProfiles {
		require.Equal(t, "", tp.Error)
		require.Equal(t, "finished", tp.State)
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
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, "profile", string(body))
		err = resp.Body.Close()
		require.NoError(t, err)
	}
}

func testAPIDownload(t *testing.T, httpAddr string, ts int64, components []topology.Component) {
	urls := []string{
		fmt.Sprintf("http://%v/continuous_profiling/download?ts=%v", httpAddr, ts),
		fmt.Sprintf("http://%v/continuous_profiling/download?begin_time=%v&end_time=%v&limit=1000", httpAddr, ts, ts),
		fmt.Sprintf("http://%v/continuous_profiling/download?ts=%v&profile_type=profile&component=%v&address=%v:%v&data_format=protobuf",
			httpAddr, ts, components[0].Name, components[0].IP, components[0].Port),
	}
	for idx, url := range urls {
		resp, err := http.Get(url)
		require.NoError(t, err)
		require.Equal(t, 200, resp.StatusCode)
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		err = resp.Body.Close()
		require.NoError(t, err)

		zr, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
		require.NoError(t, err)

		if idx == len(urls)-1 {
			require.True(t, len(zr.File) == 2) // profile file + readme.md file
		} else {
			require.True(t, len(zr.File) > len(components))
		}
		for _, f := range zr.File {
			require.True(t, f.Modified.Unix() > 0)
			if f.Name == "README.md" {
				continue
			}
			// file name format is: kind_component_ip_port_ts
			fields := strings.Split(f.Name, "_")
			require.True(t, len(fields) >= 4)
			reader, err := f.Open()
			require.NoError(t, err)

			// check content
			buf := make([]byte, len(fields[0]))
			n, _ := reader.Read(buf)
			require.Equal(t, len(fields[0]), n)
			if fields[1] == "tikv" && fields[0] == "heap" {
				require.Equal(t, "--- ", string(buf))
			} else {
				require.Equal(t, fields[0], string(buf))
			}

			if idx == len(urls)-1 {
				// test for download single profile
				require.Equal(t, fields[1], components[0].Name)
				require.Equal(t, fields[2], components[0].IP)
				continue
			}
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
}

func testAPIComponent(t *testing.T, httpAddr string, components []topology.Component) {
	resp, err := http.Get("http://" + httpAddr + "/continuous_profiling/components")
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	err = resp.Body.Close()
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
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	err = resp.Body.Close()
	require.NoError(t, err)

	var estimateSize EstimateSize
	err = json.Unmarshal(body, &estimateSize)
	require.NoError(t, err)
	require.Equal(t, len(components), estimateSize.InstanceCount, string(body))
	require.Equal(t, 106610688000, estimateSize.ProfileSize)
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
		{"/group_profiles?begin_time=1639962239&end_time=1639969440", `{"message":"query time range too large, should no more than 2 hours","status":"error"}`},

		// test for /group_profile/detail api.
		{"/group_profile/detail", `{"message":"need param ts","status":"error"}`},
		{"/group_profile/detail?ts=x", `{"message":"invalid param ts value, error: strconv.ParseInt: parsing \"x\": invalid syntax","status":"error"}`},
		{"/group_profile/detail?ts=0&limit=x", `{"message":"invalid param limit value, error: strconv.ParseInt: parsing \"x\": invalid syntax","status":"error"}`},

		// test for /single_profile/view api.
		{"/single_profile/view", `{"message":"need param ts","status":"error"}`},
		{"/single_profile/view?ts=x", `{"message":"invalid param ts value, error: strconv.ParseInt: parsing \"x\": invalid syntax","status":"error"}`},
		{"/single_profile/view?ts=0", `{"message":"need param profile_type","status":"error"}`},
		{"/single_profile/view?ts=0&data_format=svg", `{"message":"need param profile_type","status":"error"}`},
		{"/single_profile/view?ts=0&data_format=unknown", `{"message":"invalid param data_format value unknown, expected: svg, protobuf, jeprof, text","status":"error"}`},
		{"/single_profile/view?ts=0&profile_type=heap", `{"message":"need param component","status":"error"}`},
		{"/single_profile/view?ts=0&profile_type=heap&component=tidb", `{"message":"need param address","status":"error"}`},

		// test for /download api.
		{"/download", `{"message":"need param ts","status":"error"}`},
		{"/download?ts=x", `{"message":"invalid param ts value, error: strconv.ParseInt: parsing \"x\": invalid syntax","status":"error"}`},
		{"/download?begin_time=x", `{"message":"invalid param begin_time value, error: strconv.ParseInt: parsing \"x\": invalid syntax","status":"error"}`},
		{"/download?begin_time=1&end_time=x", `{"message":"invalid param end_time value, error: strconv.ParseInt: parsing \"x\": invalid syntax","status":"error"}`},
	}

	for _, ca := range cases {
		resp, err := http.Get("http://" + httpAddr + "/continuous_profiling" + ca.api)
		require.NoError(t, err)
		require.Equal(t, 503, resp.StatusCode, ca.api)
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, ca.body, string(body), ca.api)
		err = resp.Body.Close()
		require.NoError(t, err)
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

func TestQueryStatus(t *testing.T) {
	ts := testSuite{}
	ts.setup(t)
	defer ts.close(t)

	topoSubScribe := make(topology.Subscriber)
	err := conprof.Init(ts.db, topoSubScribe)
	require.NoError(t, err)
	defer conprof.Stop()

	httpAddr := setupHTTPService(t)

	storage := conprof.GetStorage()
	pt0 := meta.ProfileTarget{Kind: "goroutine", Component: "tidb", Address: "10.0.1.2"}
	pt1 := meta.ProfileTarget{Kind: "profile", Component: "tidb", Address: "10.0.1.2"}
	pt2 := meta.ProfileTarget{Kind: "heap", Component: "pd", Address: "10.0.1.2"}
	t0 := time.Now()
	t1 := t0.Add(time.Second)
	t2 := t0.Add(time.Second * 2)

	datas := []struct {
		t      time.Time
		pt     meta.ProfileTarget
		status meta.ProfileStatus
		err    error
	}{
		{t0, pt0, meta.ProfileStatusFinished, nil},
		{t0, pt1, meta.ProfileStatusFinished, nil},
		{t0, pt2, meta.ProfileStatusFinished, nil},

		{t1, pt0, meta.ProfileStatusFinished, nil},
		{t1, pt1, meta.ProfileStatusFailed, errors.New("timeout")},
		{t1, pt2, meta.ProfileStatusFinished, nil},

		{t2, pt0, meta.ProfileStatusFailed, errors.New("timeout")},
		{t2, pt1, meta.ProfileStatusFailed, errors.New("timeout")},
		{t2, pt2, meta.ProfileStatusFailed, errors.New("timeout")},
	}

	profile := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	for _, data := range datas {
		err = storage.AddProfile(data.pt, data.t, profile, data.err)
		require.NoError(t, err)
	}
	// query group profile api.
	resp, err := http.Get(fmt.Sprintf("http://%v/continuous_profiling/group_profiles?begin_time=%v&end_time=%v", httpAddr, t0.Unix(), t2.Unix()))
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	err = resp.Body.Close()
	require.NoError(t, err)
	var groupProfiles []GroupProfiles
	err = json.Unmarshal(body, &groupProfiles)
	require.NoError(t, err)
	require.Equal(t, 3, len(groupProfiles))
	require.Equal(t, t2.Unix(), groupProfiles[0].Ts)
	require.Equal(t, meta.ProfileStatusFailed.String(), groupProfiles[0].State)
	require.Equal(t, t1.Unix(), groupProfiles[1].Ts)
	require.Equal(t, meta.ProfileStatusFinishedWithError.String(), groupProfiles[1].State)
	require.Equal(t, t0.Unix(), groupProfiles[2].Ts)
	require.Equal(t, meta.ProfileStatusFinished.String(), groupProfiles[2].State)

	// query group profile detail api.
	statusList := []meta.ProfileStatus{meta.ProfileStatusFinished, meta.ProfileStatusFinishedWithError, meta.ProfileStatusFailed}
	for i, time := range []time.Time{t0, t1, t2} {
		ts := time.Unix()
		resp, err = http.Get("http://" + httpAddr + "/continuous_profiling/group_profile/detail?ts=" + strconv.Itoa(int(ts)))
		require.NoError(t, err)
		require.Equal(t, 200, resp.StatusCode)
		body, err = io.ReadAll(resp.Body)
		require.NoError(t, err)
		err = resp.Body.Close()
		require.NoError(t, err)
		var groupProfileDetail GroupProfileDetail
		err = json.Unmarshal(body, &groupProfileDetail)
		require.NoError(t, err)
		require.Equal(t, ts, groupProfileDetail.Ts)
		require.Equal(t, statusList[i].String(), groupProfileDetail.State)
		require.Equal(t, 3, len(groupProfileDetail.TargetProfiles))
		for _, tp := range groupProfileDetail.TargetProfiles {
			found := false
			for _, data := range datas {
				if tp.Target.Component == data.pt.Component && tp.Type == data.pt.Kind && ts == data.t.Unix() {
					require.Equal(t, data.status.String(), tp.State)
					found = true
					break
				}
			}
			require.True(t, found)
		}
	}
}
