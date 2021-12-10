package pdvariable_test

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/ng-monitoring/config/pdvariable"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/integration"
)

func TestPDVariableSubscribe(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create file contains a colon which is not allowed on Windows")
	}

	for i := 0; i < 2; i++ {
		testPDVariableSubscribe(t, i%2 == 0)
	}
}

func testPDVariableSubscribe(t *testing.T, init bool) {
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer cluster.Terminate(t)

	if init {
		cli := cluster.RandClient()
		_, err := cli.Put(context.Background(), pdvariable.GlobalConfigPath+"enable_resource_metering", "false")
		require.NoError(t, err)
	}

	pdvariable.Init(func() *clientv3.Client {
		return cluster.RandClient()
	})
	defer pdvariable.Stop()

	// wait for first load finish
	time.Sleep(time.Millisecond * 100)

	sub := pdvariable.Subscribe()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		cli := cluster.RandClient()
		_, err := cli.Put(context.Background(), pdvariable.GlobalConfigPath+"unknow", "false")
		require.NoError(t, err)
		_, err = cli.Put(context.Background(), pdvariable.GlobalConfigPath+"enable_resource_metering", "true")
		require.NoError(t, err)
		_, err = cli.Put(context.Background(), pdvariable.GlobalConfigPath+"unknow", "abcd")
		require.NoError(t, err)
		_, err = cli.Put(context.Background(), pdvariable.GlobalConfigPath+"enable_resource_metering", "false")
		require.NoError(t, err)
	}()

	for i, v := range []bool{true, false} {
		vars := <-sub
		require.Equal(t, v, vars.EnableTopSQL, "idx: %v", i)
	}
}
