package domain

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/ng-monitoring/utils/testutil"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*loggingT).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime.init.0.func1"),
	}

	goleak.VerifyTestMain(m, opts...)
}

func TestDomain(t *testing.T) {
	cfg := config.GetDefaultConfig()
	config.StoreGlobalConfig(cfg)
	do := NewDomain()
	do.Close()
	_, err := do.GetEtcdClient()
	require.Error(t, err, context.Canceled)
	_, err = do.GetPDClient()
	require.Error(t, err, context.Canceled)

	mockPD := testutil.MockPDHTTPServer{}
	mockPD.Setup(t)
	defer mockPD.Close(t)
	cfg.PD.Endpoints = []string{mockPD.Addr}
	config.StoreGlobalConfig(cfg)

	do = NewDomain()
	defer do.Close()

	pdCli1, err := do.GetPDClient()
	require.NoError(t, err)
	cfg.ContinueProfiling.Enable = true
	config.StoreGlobalConfig(cfg)
	pdCli2, err := do.GetPDClient()
	require.NoError(t, err)
	require.Equal(t, pdCli1, pdCli2)

	mockPD2 := testutil.MockPDHTTPServer{}
	mockPD2.Setup(t)
	defer mockPD2.Close(t)
	cfg.PD.Endpoints = []string{mockPD2.Addr}
	config.StoreGlobalConfig(cfg)
	time.Sleep(time.Millisecond * 10)
	pdCli3, err := do.GetPDClient()
	require.NoError(t, err)
	require.NotEqual(t, pdCli1, pdCli3)
}

func TestClientMaintainer(t *testing.T) {
	cfg := config.GetDefaultConfig()
	cfg.PD.Endpoints = nil
	config.StoreGlobalConfig(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	_, _, err := createClientWithRetry(ctx)
	cancel()
	require.Error(t, err, context.DeadlineExceeded)

	mockPD := testutil.MockPDHTTPServer{}
	mockPD.Setup(t)
	defer mockPD.Close(t)

	_, err = CreatePDClient(&cfg)
	require.NotNil(t, err)
	require.Equal(t, "need specify pd endpoints", err.Error())
	cfg.PD.Endpoints = []string{mockPD.Addr}
	config.StoreGlobalConfig(cfg)
	mockPD.Health = false
	_, err = CreatePDClient(&cfg)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "Response status 503")
	mockPD.Health = true
	pdCli, etcdCli, err := createClientWithRetry(context.Background())
	require.NoError(t, err)
	require.NotNil(t, pdCli)
	require.NotNil(t, etcdCli)

	cm := NewClientMaintainer()
	require.False(t, cm.IsInitialized())
	ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond*10)
	_, err = cm.GetPDClient(ctx)
	cancel()
	require.NotNil(t, err)
	require.Equal(t, context.DeadlineExceeded, err)

	ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond*10)
	_, err = cm.GetEtcdClient(ctx)
	cancel()
	require.NotNil(t, err)
	require.Equal(t, context.DeadlineExceeded, err)

	cm.Init(cfg.PD, pdCli, etcdCli)
	require.True(t, cm.IsInitialized())
	ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond*10)
	pdCli, err = cm.GetPDClient(ctx)
	cancel()
	require.NoError(t, err)
	require.NotNil(t, pdCli)

	ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond*10)
	etcdCli, err = cm.GetEtcdClient(ctx)
	cancel()
	require.NoError(t, err)
	require.NotNil(t, etcdCli)

	cm.Close()
}
