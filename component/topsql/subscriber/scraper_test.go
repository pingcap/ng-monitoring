package subscriber_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/pingcap/ng-monitoring/component/topology"
	"github.com/pingcap/ng-monitoring/component/topsql/mock"
	"github.com/pingcap/ng-monitoring/component/topsql/subscriber"
	"github.com/pingcap/ng-monitoring/utils/testutil"

	rua "github.com/pingcap/kvproto/pkg/resource_usage_agent"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestScraperTiDBBasic(t *testing.T) {
	t.Parallel()

	// insecure
	testScraperTiDBBasic(t, nil, nil)

	// tls
	serverTLS, clientTLS, err := testutil.SetupCert()
	require.NoError(t, err)
	testScraperTiDBBasic(t, serverTLS, clientTLS)
}

func TestScraperTiKVBasic(t *testing.T) {
	t.Parallel()

	// insecure
	testScraperTiKVBasic(t, nil, nil)

	// tls
	serverTLS, clientTLS, err := testutil.SetupCert()
	require.NoError(t, err)
	testScraperTiKVBasic(t, serverTLS, clientTLS)
}

func testScraperTiDBBasic(t *testing.T, serverTLS *tls.Config, clientTLS *tls.Config) {
	store := mock.NewMemStore()
	defer store.Close()

	pubsub := mock.NewMockPubSub()
	ip, port, err := pubsub.Listen("127.0.0.1:0", serverTLS)
	require.NoError(t, err)
	go pubsub.Serve()
	defer pubsub.Stop()

	component := topology.Component{
		Name:       topology.ComponentTiDB,
		IP:         ip,
		StatusPort: port,
	}
	scraper := subscriber.NewScraper(context.Background(), nil, component, store, clientTLS)
	go scraper.Run()
	defer scraper.Close()

	checkTiDBScrape(t, fmt.Sprintf("%s:%d", ip, port), pubsub, store)
}

func testScraperTiKVBasic(t *testing.T, serverTLS *tls.Config, clientTLS *tls.Config) {
	store := mock.NewMemStore()
	defer store.Close()

	pubsub := mock.NewMockPubSub()
	ip, port, err := pubsub.Listen("127.0.0.1:0", serverTLS)
	require.NoError(t, err)
	go pubsub.Serve()
	defer pubsub.Stop()

	component := topology.Component{
		Name: topology.ComponentTiKV,
		IP:   ip,
		Port: port,
	}
	scraper := subscriber.NewScraper(context.Background(), nil, component, store, clientTLS)
	go scraper.Run()
	defer scraper.Close()

	checkTiKVScrape(t, fmt.Sprintf("%s:%d", ip, port), pubsub, store)
}

func TestScraperCloseFirst(t *testing.T) {
	t.Parallel()

	store := mock.NewMemStore()
	defer store.Close()

	pubsub := mock.NewMockPubSub()
	ip, port, err := pubsub.Listen("127.0.0.1:0", nil)
	require.NoError(t, err)
	go pubsub.Serve()
	defer pubsub.Stop()

	component := topology.Component{
		Name: topology.ComponentTiDB,
		IP:   ip,
		Port: port,
	}
	scraper := subscriber.NewScraper(context.Background(), nil, component, store, nil)
	scraper.Close()
	scraper.Run()
}

func TestScraperOtherComponent(t *testing.T) {
	t.Parallel()

	store := mock.NewMemStore()
	defer store.Close()

	component := topology.Component{
		Name: topology.ComponentPD,
	}
	scraper := subscriber.NewScraper(context.Background(), nil, component, store, nil)
	require.Nil(t, scraper)
}

func TestScraperTiDBRestart(t *testing.T) {
	t.Parallel()

	store := mock.NewMemStore()
	defer store.Close()

	pubsub := mock.NewMockPubSub()
	ip, port, err := pubsub.Listen("127.0.0.1:0", nil)
	require.NoError(t, err)
	go pubsub.Serve()

	component := topology.Component{
		Name:       topology.ComponentTiDB,
		IP:         ip,
		StatusPort: port,
	}
	scraper := subscriber.NewScraper(context.Background(), nil, component, store, nil)
	go scraper.Run()
	defer scraper.Close()

	addr := fmt.Sprintf("%s:%d", ip, port)
	checkTiDBScrape(t, addr, pubsub, store)

	pubsub.Stop()
	time.Sleep(5 * time.Second)

	pubsub = mock.NewMockPubSub()
	_, _, err = pubsub.Listen(addr, nil)
	require.NoError(t, err)
	go pubsub.Serve()
	defer pubsub.Stop()
	checkTiDBScrape(t, addr, pubsub, store)
}

func TestScraperTiKVRestart(t *testing.T) {
	t.Parallel()

	store := mock.NewMemStore()
	defer store.Close()

	pubsub := mock.NewMockPubSub()
	ip, port, err := pubsub.Listen("127.0.0.1:0", nil)
	require.NoError(t, err)
	go pubsub.Serve()

	component := topology.Component{
		Name: topology.ComponentTiKV,
		IP:   ip,
		Port: port,
	}
	scraper := subscriber.NewScraper(context.Background(), nil, component, store, nil)
	go scraper.Run()
	defer scraper.Close()

	addr := fmt.Sprintf("%s:%d", ip, port)
	checkTiKVScrape(t, addr, pubsub, store)

	pubsub.Stop()
	time.Sleep(5 * time.Second)

	pubsub = mock.NewMockPubSub()
	_, _, err = pubsub.Listen(addr, nil)
	require.NoError(t, err)
	go pubsub.Serve()
	defer pubsub.Stop()
	checkTiKVScrape(t, addr, pubsub, store)
}

func checkTiDBScrape(t *testing.T, addr string, pubsub *mock.MockPubSub, store *mock.MemStore) {
	rand.Seed(time.Now().Unix())
	tsSec := rand.Uint64()
	cpuTimeMs := rand.Uint32()
	meta := rand.Int()
	sqlDigest := fmt.Sprintf("mock_sql_digest_%d", meta)
	sqlText := fmt.Sprintf("mock_normalized_sql_%d", meta)
	planDigest := fmt.Sprintf("mock_plan_digest_%d", meta)
	planText := fmt.Sprintf("mock__normalized_plan_%d", meta)

	pubsub.AccessTiDBStream(func(stream tipb.TopSQLPubSub_SubscribeServer) error {
		require.NoError(t, stream.Send(&tipb.TopSQLSubResponse{RespOneof: &tipb.TopSQLSubResponse_Record{
			Record: &tipb.TopSQLRecord{
				SqlDigest:  []byte(sqlDigest),
				PlanDigest: []byte(planDigest),
				Items:      []*tipb.TopSQLRecordItem{{TimestampSec: tsSec, CpuTimeMs: cpuTimeMs}},
			},
		}}))

		require.NoError(t, stream.Send(&tipb.TopSQLSubResponse{RespOneof: &tipb.TopSQLSubResponse_SqlMeta{
			SqlMeta: &tipb.SQLMeta{
				SqlDigest:     []byte(sqlDigest),
				NormalizedSql: sqlText,
			},
		}}))

		require.NoError(t, stream.Send(&tipb.TopSQLSubResponse{RespOneof: &tipb.TopSQLSubResponse_PlanMeta{
			PlanMeta: &tipb.PlanMeta{
				PlanDigest:     []byte(planDigest),
				NormalizedPlan: planText,
			},
		}}))
		return nil
	})

	require.True(t, store.Pred(func(store *mock.MemStore) bool {
		if _, ok := store.TopSQLRecords[addr]; !ok {
			return false
		}
		if _, ok := store.TopSQLRecords[addr][sqlDigest]; !ok {
			return false
		}
		if _, ok := store.TopSQLRecords[addr][sqlDigest][planDigest]; !ok {
			return false
		}
		if _, ok := store.SQLMetas[sqlDigest]; !ok {
			return false
		}
		if _, ok := store.PlanMetas[planDigest]; !ok {
			return false
		}

		require.Equal(t, store.SQLMetas[sqlDigest].Meta.NormalizedSql, sqlText)
		require.Equal(t, store.PlanMetas[planDigest].Meta.NormalizedPlan, planText)
		record := store.TopSQLRecords[addr][sqlDigest][planDigest]
		got := false
		for _, i := range record.Items {
			if i.TimestampSec == tsSec {
				got = true
				require.Equal(t, cpuTimeMs, i.CpuTimeMs)
			}
		}
		require.True(t, got)
		return true
	}, 10*time.Millisecond, 1*time.Second))
}

func checkTiKVScrape(t *testing.T, addr string, pubsub *mock.MockPubSub, store *mock.MemStore) {
	rand.Seed(time.Now().Unix())
	tsSec := rand.Uint64()
	cpuMs := rand.Uint32()
	rdKeys := rand.Uint32()
	wtKeys := rand.Uint32()
	tag := fmt.Sprintf("mock_resource_group_tag_%d", rand.Int())

	pubsub.AccessTiKVStream(func(stream rua.ResourceMeteringPubSub_SubscribeServer) error {
		return stream.Send(&rua.ResourceUsageRecord{
			RecordOneof: &rua.ResourceUsageRecord_Record{
				Record: &rua.GroupTagRecord{
					ResourceGroupTag: []byte(tag),
					Items: []*rua.GroupTagRecordItem{{
						TimestampSec: tsSec,
						CpuTimeMs:    cpuMs,
						ReadKeys:     rdKeys,
						WriteKeys:    wtKeys,
					}},
				},
			},
		})
	})

	require.True(t, store.Pred(func(store *mock.MemStore) bool {
		if _, ok := store.ResourceMeteringRecords[addr]; !ok {
			return false
		}
		if _, ok := store.ResourceMeteringRecords[addr][tag]; !ok {
			return false
		}

		record := store.ResourceMeteringRecords[addr][tag]
		got := false
		for _, item := range record.GetRecord().GetItems() {
			if item.TimestampSec == tsSec {
				got = true
				require.Equal(t, item.CpuTimeMs, cpuMs)
				require.Equal(t, item.ReadKeys, rdKeys)
				require.Equal(t, item.WriteKeys, wtKeys)
			}
		}
		require.True(t, got)
		return true
	}, 10*time.Millisecond, 1*time.Second))
}
