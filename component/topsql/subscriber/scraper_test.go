package subscriber_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	rua "github.com/pingcap/kvproto/pkg/resource_usage_agent"
	"github.com/pingcap/ng-monitoring/component/topology"
	"github.com/pingcap/ng-monitoring/component/topsql/mock"
	"github.com/pingcap/ng-monitoring/component/topsql/mock/pubsub"
	"github.com/pingcap/ng-monitoring/component/topsql/subscriber"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestTiDBScraperBasic(t *testing.T) {
	t.Parallel()

	store := mock.NewMemStore()
	defer store.Close()

	tidb := pubsub.NewMockTiDBPubSub()
	ip, port, err := tidb.Listen()
	require.NoError(t, err)
	go tidb.Serve()
	defer tidb.Stop()

	component := topology.Component{
		Name:       "tidb",
		IP:         ip,
		StatusPort: port,
	}
	scraper := subscriber.NewScraper(context.Background(), component, store)
	go scraper.Run()
	defer scraper.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	tidb.AccessStream(func(stream tipb.TopSQLPubSub_SubscribeServer) error {
		defer wg.Done()
		require.NoError(t, stream.Send(&tipb.TopSQLSubResponse{RespOneof: &tipb.TopSQLSubResponse_Record{
			Record: &tipb.CPUTimeRecord{
				SqlDigest:              []byte("mock_sql_digest"),
				PlanDigest:             []byte("mock_plan_digest"),
				RecordListTimestampSec: []uint64{1639541002},
				RecordListCpuTimeMs:    []uint32{100},
			},
		}}))

		require.NoError(t, stream.Send(&tipb.TopSQLSubResponse{RespOneof: &tipb.TopSQLSubResponse_SqlMeta{
			SqlMeta: &tipb.SQLMeta{
				SqlDigest:     []byte("mock_sql_digest"),
				NormalizedSql: "mock_normalized_sql",
			},
		}}))

		require.NoError(t, stream.Send(&tipb.TopSQLSubResponse{RespOneof: &tipb.TopSQLSubResponse_PlanMeta{
			PlanMeta: &tipb.PlanMeta{
				PlanDigest:     []byte("mock_plan_digest"),
				NormalizedPlan: "mock_normalized_plan",
			},
		}}))
		return nil
	})
	wg.Wait()

	require.True(t, store.Predict(func(store *mock.MemStore) bool {
		addr := fmt.Sprintf("%s:%d", ip, port)

		if _, ok := store.Instances[addr]; !ok {
			return false
		}
		if _, ok := store.TopSQLRecords[addr]; !ok {
			return false
		}
		if _, ok := store.SQLMetas["mock_sql_digest"]; !ok {
			return false
		}
		if _, ok := store.PlanMetas["mock_plan_digest"]; !ok {
			return false
		}

		require.Equal(t, store.Instances[addr].InstanceType, "tidb")
		record := store.TopSQLRecords[addr]["mock_sql_digest"]["mock_plan_digest"]
		require.Equal(t, record.RecordListTimestampSec, []uint64{1639541002})
		require.Equal(t, record.RecordListCpuTimeMs, []uint32{100})
		require.Equal(t, store.SQLMetas["mock_sql_digest"].Meta.NormalizedSql, "mock_normalized_sql")
		require.Equal(t, store.PlanMetas["mock_plan_digest"].Meta.NormalizedPlan, "mock_normalized_plan")
		return true
	}, 10*time.Millisecond, 1*time.Second))
}

func TestTiKVScraperBasic(t *testing.T) {
	t.Parallel()

	store := mock.NewMemStore()
	defer store.Close()

	tikv := pubsub.NewMockTiKVPubSub()
	ip, port, err := tikv.Listen()
	require.NoError(t, err)
	go tikv.Serve()
	defer tikv.Stop()

	component := topology.Component{
		Name: "tikv",
		IP:   ip,
		Port: port,
	}
	scraper := subscriber.NewScraper(context.Background(), component, store)
	go scraper.Run()
	defer scraper.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	tikv.AccessStream(func(stream rua.ResourceMeteringPubSub_SubscribeServer) error {
		defer wg.Done()
		return stream.Send(&rua.ResourceUsageRecord{
			ResourceGroupTag:       []byte("mock_resource_group_tag"),
			RecordListTimestampSec: []uint64{1639541002},
			RecordListCpuTimeMs:    []uint32{100},
			RecordListReadKeys:     []uint32{200},
			RecordListWriteKeys:    []uint32{300},
		})
	})
	wg.Wait()

	require.True(t, store.Predict(func(store *mock.MemStore) bool {
		addr := fmt.Sprintf("%s:%d", ip, port)

		if _, ok := store.Instances[addr]; !ok {
			return false
		}
		if _, ok := store.ResourceMeteringRecords[addr]; !ok {
			return false
		}

		require.Equal(t, store.Instances[addr].InstanceType, "tikv")
		record := store.ResourceMeteringRecords[addr]["mock_resource_group_tag"]
		require.Equal(t, record.RecordListTimestampSec, []uint64{1639541002})
		require.Equal(t, record.RecordListCpuTimeMs, []uint32{100})
		require.Equal(t, record.RecordListReadKeys, []uint32{200})
		require.Equal(t, record.RecordListWriteKeys, []uint32{300})

		return true
	}, 10*time.Millisecond, 1*time.Second))
}
