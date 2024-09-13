package subscriber_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/ng-monitoring/component/subscriber"
	"github.com/pingcap/ng-monitoring/component/topology"
	"github.com/pingcap/ng-monitoring/component/topsql/mock"
	sub "github.com/pingcap/ng-monitoring/component/topsql/subscriber"
	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/ng-monitoring/config/pdvariable"

	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

type testSuite struct {
	subscriber *subscriber.Subscriber

	varSubscriber  pdvariable.Subscriber
	topoSubscriber topology.Subscriber
	cfgSubscriber  config.Subscriber

	store   *mock.MemStore
	service *mock.MockPubSub

	ip   string
	port uint
}

func newTestSuite() *testSuite {
	ts := &testSuite{}

	ts.varSubscriber = make(pdvariable.Subscriber)
	ts.topoSubscriber = make(topology.Subscriber)
	ts.cfgSubscriber = make(config.Subscriber)
	ts.store = mock.NewMemStore()

	controller := sub.NewSubscriberController(ts.store)
	ts.subscriber = subscriber.NewSubscriber(nil, ts.topoSubscriber, ts.varSubscriber, ts.cfgSubscriber, controller)

	ts.service = mock.NewMockPubSub()
	ts.ip, ts.port, _ = ts.service.Listen("127.0.0.1:0", nil)
	go ts.service.Serve()
	return ts
}

func (ts *testSuite) checkTiDBScrape(t *testing.T) {
	checkTiDBScrape(t, fmt.Sprintf("%s:%d", ts.ip, ts.port), ts.service, ts.store)
}

func (ts *testSuite) checkTiKVScrape(t *testing.T) {
	checkTiKVScrape(t, fmt.Sprintf("%s:%d", ts.ip, ts.port), ts.service, ts.store)
}

func (ts *testSuite) Close() {
	ts.service.Stop()
	ts.subscriber.Close()
	ts.store.Close()
}

func TestSubscriberBasic(t *testing.T) {
	t.Parallel()

	ts := newTestSuite()
	defer ts.Close()

	ts.varSubscriber <- enable
	topo := []topology.Component{{
		Name:       topology.ComponentTiDB,
		IP:         ts.ip,
		StatusPort: ts.port,
	}, {
		Name: topology.ComponentPD,
	}, {
		Name: topology.ComponentTiFlash,
	}}
	ts.topoSubscriber <- topoGetter(topo)
	ts.checkTiDBScrape(t)

	topo = append(topo, topology.Component{
		Name: topology.ComponentTiKV,
		IP:   ts.ip,
		Port: ts.port,
	})
	ts.topoSubscriber <- topoGetter(topo)
	ts.checkTiKVScrape(t)
}

func TestSubscriberEnableAfterTopoIsReady(t *testing.T) {
	t.Parallel()

	ts := newTestSuite()
	defer ts.Close()

	topo := []topology.Component{{
		Name:       topology.ComponentTiDB,
		IP:         ts.ip,
		StatusPort: ts.port,
	}}
	ts.topoSubscriber <- topoGetter(topo)
	ts.varSubscriber <- enable
	ts.checkTiDBScrape(t)
}

func TestSubscriberTopoChange(t *testing.T) {
	t.Parallel()

	ts := newTestSuite()
	defer ts.Close()

	ts.varSubscriber <- enable
	topo := []topology.Component{{
		Name:       topology.ComponentTiDB,
		IP:         ts.ip,
		StatusPort: ts.port,
	}, {
		Name: topology.ComponentTiKV,
		IP:   ts.ip,
		Port: ts.port,
	}}
	ts.topoSubscriber <- topoGetter(topo)

	ts.checkTiDBScrape(t)
	ts.checkTiKVScrape(t)

	// tidb component is out
	ts.service.AccessTiDBStream(func(s tipb.TopSQLPubSub_SubscribeServer) error {
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
	ts.topoSubscriber <- topoGetter(topo[1:])
}

func TestSubscriberDisable(t *testing.T) {
	t.Parallel()

	ts := newTestSuite()
	defer ts.Close()

	ts.varSubscriber <- enable
	topo := []topology.Component{{
		Name:       topology.ComponentTiDB,
		IP:         ts.ip,
		StatusPort: ts.port,
	}}
	ts.topoSubscriber <- topoGetter(topo)
	ts.checkTiDBScrape(t)

	// disable
	ts.service.AccessTiDBStream(func(s tipb.TopSQLPubSub_SubscribeServer) error {
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

func enable() pdvariable.PDVariable {
	return pdvariable.PDVariable{EnableTopSQL: true}
}

func disable() pdvariable.PDVariable {
	return pdvariable.PDVariable{EnableTopSQL: false}
}

func topoGetter(topo []topology.Component) topology.GetLatestTopology {
	return func() []topology.Component {
		return topo
	}
}
