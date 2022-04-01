package subscriber_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/ng-monitoring/component/topology"
	"github.com/pingcap/ng-monitoring/component/topsql/mock"
	"github.com/pingcap/ng-monitoring/component/topsql/subscriber"
	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/ng-monitoring/config/pdvariable"
	"github.com/stretchr/testify/require"
)

func TestDefaultSubscriberBasic(t *testing.T) {
	t.Parallel()

	store := mock.NewMemStore()
	defer store.Close()

	cfg := config.GetDefaultConfig()
	varSubscriber := make(pdvariable.Subscriber)
	topoSubscriber := make(topology.Subscriber)
	cfgSubscriber := make(config.Subscriber)
	sub := subscriber.NewDefaultSubscriber(&cfg, topoSubscriber, varSubscriber, cfgSubscriber, store)
	defer sub.Close()

	pubsub := mock.NewMockPubSub()
	ip, port, err := pubsub.Listen("127.0.0.1:0", nil)
	require.NoError(t, err)
	go pubsub.Serve()
	defer pubsub.Stop()

	varSubscriber <- enable
	topo := []topology.Component{{
		Name:       "tidb",
		IP:         ip,
		StatusPort: port,
	}}
	topoSubscriber <- topo
	checkTiDBScrape(t, fmt.Sprintf("%s:%d", ip, port), pubsub, store)
}

func enable() pdvariable.PDVariable {
	return pdvariable.PDVariable{EnableTopSQL: true}
}

func disable() pdvariable.PDVariable {
	return pdvariable.PDVariable{EnableTopSQL: false}
}
