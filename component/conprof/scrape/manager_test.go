package scrape

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ng_monitoring/component/conprof/meta"
	"github.com/pingcap/ng_monitoring/component/conprof/store"
	"github.com/pingcap/ng_monitoring/component/conprof/util"
	"github.com/pingcap/ng_monitoring/component/topology"
	"github.com/pingcap/ng_monitoring/config"
	"github.com/pingcap/ng_monitoring/utils/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/zap"
)

func TestMain(m *testing.M) {
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*loggingT).flushDaemon"),
	}

	goleak.VerifyTestMain(m, opts...)
}

func TestManager(t *testing.T) {
	tmpDir, err := ioutil.TempDir(os.TempDir(), "ngm-test-.*")
	require.NoError(t, err)
	defer func() {
		err := os.RemoveAll(tmpDir)
		require.NoError(t, err)
	}()

	cfg := config.GetDefaultConfig()
	cfg.ContinueProfiling.Enable = true
	cfg.ContinueProfiling.ProfileSeconds = 1
	cfg.ContinueProfiling.IntervalSeconds = 1
	config.StoreGlobalConfig(&cfg)

	db := testutil.NewGenjiDB(t, tmpDir)
	defer db.Close()
	storage, err := store.NewProfileStorage(db)
	require.NoError(t, err)

	topoSubScribe := make(topology.Subscriber)
	updateTargetMetaInterval = time.Millisecond * 100
	manager := NewManager(storage, topoSubScribe)
	manager.Start()
	defer manager.Close()

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

	t1 := time.Now()
	// wait for scrape finish
	time.Sleep(time.Millisecond * 1500)

	t2 := time.Now()
	param := &meta.BasicQueryParam{
		Begin:   util.GetTimeStamp(t1),
		End:     util.GetTimeStamp(t2),
		Limit:   1000,
		Targets: nil,
	}
	plists, err := storage.QueryGroupProfiles(param)
	require.NoError(t, err)
	checkListData := func(plists []meta.ProfileList, components []topology.Component, param *meta.BasicQueryParam) {
		require.True(t, len(plists) > len(components))
		maxTs := int64(0)
		for _, list := range plists {
			found := false
			for _, comp := range components {
				if list.Target.Component == comp.Name && list.Target.Address == fmt.Sprintf("%v:%v", comp.IP, comp.Port) {
					found = true
					break
				}
			}
			// TODO(crazycs): remove this after support tiflash
			require.True(t, list.Target.Component != topology.ComponentTiFlash)
			require.True(t, found, fmt.Sprintf("%#v", list))
			for _, ts := range list.TsList {
				require.True(t, ts >= param.Begin && ts <= param.End)
				if ts > maxTs {
					maxTs = ts
				}
			}
		}
		require.True(t, maxTs > 0)
	}
	checkListData(plists, components, param)

	// test for scrape profile data
	count := 0
	err = storage.QueryProfileData(param, func(target meta.ProfileTarget, i int64, data []byte) error {
		count++
		found := false
		for _, comp := range components {
			if target.Component == comp.Name && target.Address == fmt.Sprintf("%v:%v", comp.IP, comp.Port) {
				found = true
				break
			}
		}
		require.True(t, found, fmt.Sprintf("%#v", target))
		require.Equal(t, target.Kind, string(data))
		return nil
	})
	require.True(t, count > len(components))
	require.NoError(t, err)

	// test for update target meta.
	for _, list := range plists {
		info := storage.GetTargetInfoFromCache(list.Target)
		require.NotNil(t, info)
		require.True(t, info.ID > 0)
		require.True(t, info.LastScrapeTs >= util.GetTimeStamp(t1))
	}

	// test for GetCurrentScrapeComponents
	comp := manager.GetCurrentScrapeComponents()
	// // TODO(crazycs): update this after support tiflash
	require.Equal(t, len(comp), len(components)-1)

	// test for topology changed.
	mockServer2 := testutil.CreateMockProfileServer(t)
	defer mockServer2.Stop(t)
	addr2 := mockServer2.Addr
	port2 := mockServer2.Port
	log.Info("new mock server", zap.Uint("port", port2))
	components = []topology.Component{
		{Name: topology.ComponentPD, IP: addr2, Port: port2, StatusPort: port2},
		{Name: topology.ComponentTiDB, IP: addr, Port: port, StatusPort: port},
		{Name: topology.ComponentTiDB, IP: addr2, Port: port2, StatusPort: port2},
		{Name: topology.ComponentTiKV, IP: addr2, Port: port2, StatusPort: port2},
	}

	// mock for disable conprof
	cfg.ContinueProfiling.Enable = false
	config.StoreGlobalConfig(&cfg)

	// notify topology
	topoSubScribe <- components

	// wait for stop scrape
	time.Sleep(time.Millisecond * 100)

	// currently, shouldn't have any scrape component.
	comp = manager.GetCurrentScrapeComponents()
	require.Equal(t, len(comp), 0)

	cfg.ContinueProfiling.Enable = true
	config.StoreGlobalConfig(&cfg)
	// renotify topology
	topoSubScribe <- components
	// wait for scrape finish
	time.Sleep(time.Millisecond * 3000)

	t3 := time.Now()
	param = &meta.BasicQueryParam{
		Begin:   util.GetTimeStamp(t3) - 1,
		End:     util.GetTimeStamp(t3),
		Limit:   1000,
		Targets: nil,
	}
	plists, err = storage.QueryGroupProfiles(param)
	require.NoError(t, err)
	checkListData(plists, components, param)

	comp = manager.GetCurrentScrapeComponents()
	require.Equal(t, len(comp), len(components), fmt.Sprintf("%#v \n %#v", comp, components))
}