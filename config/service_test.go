package config

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/procutil"
	"github.com/gin-gonic/gin"
	"github.com/pingcap/ng-monitoring/database/docdb"
	"github.com/pingcap/ng-monitoring/utils/testutil"
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
	def := GetDefaultConfig()
	StoreGlobalConfig(def)
	err = LoadConfigFromStorage(context.Background(), ts.db)
	require.NoError(t, err)
}

func (ts *testSuite) close(t *testing.T) {
	err := ts.db.Close()
	require.NoError(t, err)
	err = os.RemoveAll(ts.tmpDir)
	require.NoError(t, err)
}

func TestHTTPService(t *testing.T) {
	ts := testSuite{}
	ts.setup(t)
	defer ts.close(t)
	addr := setupHTTPService(t, ts.db)
	resp, err := http.Get("http://" + addr + "/config")
	require.NoError(t, err)
	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	err = resp.Body.Close()
	require.NoError(t, err)
	cfg := Config{}
	require.Equal(t, len(data) > 10, true)
	err = json.Unmarshal(data, &cfg)
	require.NoError(t, err)

	res, err := http.Post("http://"+addr+"/config", "application/json", bytes.NewReader([]byte(`{"continuous_profiling": {"enable": true,"profile_seconds":6,"interval_seconds":11}}`)))
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)
	globalCfg := GetGlobalConfig()
	require.Equal(t, true, globalCfg.ContinueProfiling.Enable)
	require.Equal(t, 6, globalCfg.ContinueProfiling.ProfileSeconds)
	require.Equal(t, 11, globalCfg.ContinueProfiling.IntervalSeconds)
	err = res.Body.Close()
	require.NoError(t, err)

	// test for post invalid config
	res, err = http.Post("http://"+addr+"/config", "application/json", bytes.NewReader([]byte(`{"continuous_profiling": {"enable": true,"profile_seconds":1000,"interval_seconds":11}}`)))
	require.NoError(t, err)
	require.Equal(t, 503, res.StatusCode)
	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, `{"message":"new config is invalid: {\"data_retention_seconds\":259200,\"enable\":true,\"interval_seconds\":11,\"profile_seconds\":1000,\"timeout_seconds\":120}","status":"error"}`, string(body))
	err = res.Body.Close()
	require.NoError(t, err)

	// test empty body config
	res, err = http.Post("http://"+addr+"/config", "application/json", bytes.NewReader([]byte(``)))
	require.NoError(t, err)
	require.Equal(t, 503, res.StatusCode)
	body, err = io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, `{"message":"EOF","status":"error"}`, string(body))
	err = res.Body.Close()
	require.NoError(t, err)

	// test unknown config
	res, err = http.Post("http://"+addr+"/config", "application/json", bytes.NewReader([]byte(`{"unknown_module": {"enable": true}}`)))
	require.NoError(t, err)
	require.Equal(t, 503, res.StatusCode)
	body, err = io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, `{"message":"config unknown_module not support modify or unknow","status":"error"}`, string(body))
	err = res.Body.Close()
	require.NoError(t, err)

	globalCfg = GetGlobalConfig()
	require.Equal(t, true, globalCfg.ContinueProfiling.Enable)
	require.Equal(t, 6, globalCfg.ContinueProfiling.ProfileSeconds)
	require.Equal(t, 11, globalCfg.ContinueProfiling.IntervalSeconds)
}

func TestCombineHTTPWithFile(t *testing.T) {
	ts := testSuite{}
	ts.setup(t)
	defer ts.close(t)
	addr := setupHTTPService(t, ts.db)

	cfgFileName := "test-cfg.toml"
	err := os.WriteFile(cfgFileName, []byte(""), 0666)
	require.NoError(t, err)
	defer os.Remove(cfgFileName)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ReloadRoutine(ctx, cfgFileName)

	res, err := http.Post("http://"+addr+"/config", "application/json", bytes.NewReader([]byte(`{"continuous_profiling": {"enable": true}}`)))
	require.NoError(t, err)
	require.NoError(t, res.Body.Close())

	time.Sleep(100 * time.Millisecond)
	cfg := GetGlobalConfig()
	require.Equal(t, cfg.ContinueProfiling.Enable, true)

	err = os.WriteFile(cfgFileName, []byte("[pd]\nendpoints = [\"10.0.1.8:2379\"]"), 0666)
	require.NoError(t, err)
	procutil.SelfSIGHUP()

	time.Sleep(100 * time.Millisecond)
	cfg = GetGlobalConfig()
	require.Equal(t, cfg.ContinueProfiling.Enable, true)
	require.Equal(t, cfg.PD.Endpoints, []string{"10.0.1.8:2379"})

	res, err = http.Post("http://"+addr+"/config", "application/json", bytes.NewReader([]byte(`{"continuous_profiling": {"enable": false}}`)))
	require.NoError(t, err)
	require.NoError(t, res.Body.Close())

	time.Sleep(100 * time.Millisecond)
	cfg = GetGlobalConfig()
	require.Equal(t, cfg.ContinueProfiling.Enable, false)
	require.Equal(t, cfg.PD.Endpoints, []string{"10.0.1.8:2379"})

	err = os.WriteFile(cfgFileName, []byte("[pd]\nendpoints = [\"10.0.1.8:2479\"]"), 0666)
	require.NoError(t, err)
	procutil.SelfSIGHUP()

	time.Sleep(100 * time.Millisecond)
	cfg = GetGlobalConfig()
	require.Equal(t, cfg.ContinueProfiling.Enable, false)
	require.Equal(t, cfg.PD.Endpoints, []string{"10.0.1.8:2479"})
}

func setupHTTPService(t *testing.T, docDB docdb.DocDB) string {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	gin.SetMode(gin.ReleaseMode)
	ng := gin.New()

	ng.Use(gin.Recovery())
	configGroup := ng.Group("/config")
	HTTPService(configGroup, docDB)
	httpServer := &http.Server{Handler: ng}

	go func() {
		if err = httpServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			require.NoError(t, err)
		}
	}()
	return listener.Addr().String()
}
