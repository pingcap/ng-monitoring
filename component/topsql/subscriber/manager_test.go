package subscriber_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/ng-monitoring/component/topology"
	"github.com/pingcap/ng-monitoring/component/topsql/mock"
	"github.com/pingcap/ng-monitoring/component/topsql/subscriber"
	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/ng-monitoring/config/pdvariable"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

type testSuite struct {
	manager *subscriber.Manager

	varSubscriber  pdvariable.Subscriber
	topoSubscriber topology.Subscriber
	cfgSubscriber  config.Subscriber

	store *mock.MemStore
	wg    sync.WaitGroup
}

func newTestSuite() *testSuite {
	ts := &testSuite{}

	ts.varSubscriber = make(pdvariable.Subscriber)
	ts.topoSubscriber = make(topology.Subscriber)
	ts.cfgSubscriber = make(config.Subscriber)
	ts.store = mock.NewMemStore()

	ts.manager = subscriber.NewManager(context.Background(), &ts.wg, ts.varSubscriber, ts.topoSubscriber, ts.cfgSubscriber, ts.store)
	go ts.manager.Run()

	return ts
}

func (t *testSuite) Close() {
	t.manager.Close()
	t.store.Close()
	t.wg.Wait()
}

func TestManagerBasic(t *testing.T) {
	t.Parallel()

	ts := newTestSuite()
	defer ts.Close()

	pubsub := mock.NewMockPubSub()
	ip, port, err := pubsub.Listen("127.0.0.1:0", nil)
	require.NoError(t, err)
	go pubsub.Serve()
	defer pubsub.Stop()

	ts.varSubscriber <- enable
	topo := []topology.Component{{
		Name:       "tidb",
		IP:         ip,
		StatusPort: port,
	}}
	ts.topoSubscriber <- topo
	checkTiDBScrape(t, fmt.Sprintf("%s:%d", ip, port), pubsub, ts.store)

	topo = append(topo, topology.Component{
		Name: "tikv",
		IP:   ip,
		Port: port,
	})
	ts.topoSubscriber <- topo
	checkTiKVScrape(t, fmt.Sprintf("%s:%d", ip, port), pubsub, ts.store)
}

func TestManagerEnableAfterTopoIsReady(t *testing.T) {
	t.Parallel()

	ts := newTestSuite()
	defer ts.Close()

	pubsub := mock.NewMockPubSub()
	ip, port, err := pubsub.Listen("127.0.0.1:0", nil)
	require.NoError(t, err)
	go pubsub.Serve()
	defer pubsub.Stop()

	topo := []topology.Component{{
		Name:       "tidb",
		IP:         ip,
		StatusPort: port,
	}}
	ts.topoSubscriber <- topo

	ts.varSubscriber <- enable
	checkTiDBScrape(t, fmt.Sprintf("%s:%d", ip, port), pubsub, ts.store)
}

func TestManagerTopoChange(t *testing.T) {
	t.Parallel()

	ts := newTestSuite()
	defer ts.Close()

	pubsub := mock.NewMockPubSub()
	ip, port, err := pubsub.Listen("127.0.0.1:0", nil)
	require.NoError(t, err)
	go pubsub.Serve()
	defer pubsub.Stop()

	ts.varSubscriber <- enable
	topo := []topology.Component{{
		Name:       "tidb",
		IP:         ip,
		StatusPort: port,
	}, {
		Name: "tikv",
		IP:   ip,
		Port: port,
	}}
	ts.topoSubscriber <- topo

	checkTiDBScrape(t, fmt.Sprintf("%s:%d", ip, port), pubsub, ts.store)
	checkTiKVScrape(t, fmt.Sprintf("%s:%d", ip, port), pubsub, ts.store)

	// tidb component is out
	pubsub.AccessTiDBStream(func(s tipb.TopSQLPubSub_SubscribeServer) error {
		retry := 0
		for {
			err := s.Send(&tipb.TopSQLSubResponse{})
			if err != nil && strings.Contains(err.Error(), "transport is closing") {
				return nil
			}

			if retry > 5 {
				require.Fail(t, "err should not be nil due to scraper should be closed")
			}
			retry += 1
			time.Sleep(10 * time.Millisecond)
		}
	})
	ts.topoSubscriber <- topo[1:]
}

func TestManagerDisable(t *testing.T) {
	t.Parallel()

	ts := newTestSuite()
	defer ts.Close()

	pubsub := mock.NewMockPubSub()
	ip, port, err := pubsub.Listen("127.0.0.1:0", nil)
	require.NoError(t, err)
	go pubsub.Serve()
	defer pubsub.Stop()

	ts.varSubscriber <- enable
	topo := []topology.Component{{
		Name:       "tidb",
		IP:         ip,
		StatusPort: port,
	}}
	ts.topoSubscriber <- topo
	checkTiDBScrape(t, fmt.Sprintf("%s:%d", ip, port), pubsub, ts.store)

	// disable
	pubsub.AccessTiDBStream(func(s tipb.TopSQLPubSub_SubscribeServer) error {
		retry := 0
		for {
			err := s.Send(&tipb.TopSQLSubResponse{})
			if err != nil && strings.Contains(err.Error(), "transport is closing") {
				return nil
			}

			if retry > 5 {
				require.Fail(t, "err should not be nil due to scraper should be closed")
			}
			retry += 1
			time.Sleep(10 * time.Millisecond)
		}
	})
	ts.varSubscriber <- disable
}
