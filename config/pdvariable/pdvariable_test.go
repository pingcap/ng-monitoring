package pdvariable_test

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/pingcap/ng-monitoring/component/domain"
	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/ng-monitoring/config/pdvariable"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/tests/v3/integration"
)

func TestPDVariableSubscribe(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create file contains a colon which is not allowed on Windows")
	}

	integration.BeforeTestExternal(t)
	for i := 0; i < 2; i++ {
		testPDVariableSubscribe(t, i%2 == 0)
	}
}

func testPDVariableSubscribe(t *testing.T, init bool) {
	cfg := config.GetDefaultConfig()
	config.StoreGlobalConfig(cfg)

	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer cluster.Terminate(t)

	if init {
		cli := cluster.RandClient()
		_, err := cli.Put(context.Background(), pdvariable.GlobalConfigPath+"enable_resource_metering", "false")
		require.NoError(t, err)
	}

	do := domain.NewDomainForTest(nil, cluster.RandClient())
	pdvariable.Init(do)
	defer pdvariable.Stop()

	// wait for first load finish
	time.Sleep(time.Millisecond * 100)

	sub := pdvariable.Subscribe()
	getVars := <-sub
	require.Equal(t, false, getVars().EnableTopSQL)

	cli := cluster.RandClient()
	_, err := cli.Put(context.Background(), pdvariable.GlobalConfigPath+"unknown", "false")
	require.NoError(t, err)
	_, err = cli.Put(context.Background(), pdvariable.GlobalConfigPath+"unknown", "abcd")
	require.NoError(t, err)
	_, err = cli.Put(context.Background(), pdvariable.GlobalConfigPath+"enable_resource_metering", "true")
	require.NoError(t, err)

	time.Sleep(time.Millisecond * 100)
	getVars = <-sub
	require.Equal(t, true, getVars().EnableTopSQL)

	cli = cluster.RandClient()
	_, err = cli.Put(context.Background(), pdvariable.GlobalConfigPath+"enable_resource_metering", "false")
	require.NoError(t, err)
	_, err = cli.Put(context.Background(), pdvariable.GlobalConfigPath+"enable_resource_metering", "false")
	require.NoError(t, err)
	_, err = cli.Put(context.Background(), pdvariable.GlobalConfigPath+"enable_resource_metering", "true")
	require.NoError(t, err)

	time.Sleep(time.Millisecond * 100)
	getVars = <-sub
	require.Equal(t, true, getVars().EnableTopSQL)

	cli = cluster.RandClient()
	_, err = cli.Put(context.Background(), pdvariable.GlobalConfigPath+"enable_resource_metering", "true")
	require.NoError(t, err)
	_, err = cli.Put(context.Background(), pdvariable.GlobalConfigPath+"enable_resource_metering", "false")
	require.NoError(t, err)
	_, err = cli.Put(context.Background(), pdvariable.GlobalConfigPath+"enable_resource_metering", "true")
	require.NoError(t, err)

	time.Sleep(time.Millisecond * 100)
	getVars = <-sub
	require.Equal(t, true, getVars().EnableTopSQL)

	cli = cluster.RandClient()
	_, err = cli.Put(context.Background(), pdvariable.GlobalConfigPath+"enable_resource_metering", "false")
	require.NoError(t, err)
	_, err = cli.Put(context.Background(), pdvariable.GlobalConfigPath+"enable_resource_metering", "true")
	require.NoError(t, err)
	_, err = cli.Put(context.Background(), pdvariable.GlobalConfigPath+"enable_resource_metering", "false")
	require.NoError(t, err)

	time.Sleep(time.Millisecond * 100)
	getVars = <-sub
	require.Equal(t, false, getVars().EnableTopSQL)
}
