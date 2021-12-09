package subscriber

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/pingcap/ng_monitoring/component/topology"
	"github.com/pingcap/ng_monitoring/component/topsql/store"
	"github.com/pingcap/ng_monitoring/config"
	"github.com/pingcap/ng_monitoring/config/pdvariable"
	"github.com/pingcap/ng_monitoring/utils"

	"github.com/pingcap/kvproto/pkg/resource_usage_agent"
	"github.com/pingcap/log"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

var (
	dialTimeout = 5 * time.Second
)

var (
	globalStopCh chan struct{}
	scraperWG    sync.WaitGroup
)

func Init(topoSubscriber topology.Subscriber, varSubscriber pdvariable.Subscriber) {
	globalStopCh = make(chan struct{})

	scraperWG.Add(1)
	go utils.GoWithRecovery(func() {
		defer scraperWG.Done()
		sm := Manager{
			varSubscriber:  varSubscriber,
			subscribers:    make(map[topology.Component]*Subscriber),
			topoSubscriber: topoSubscriber,
		}
		sm.run()
	}, nil)
}

func Stop() {
	log.Info("stopping subscribers")
	close(globalStopCh)
	scraperWG.Wait()
	log.Info("stop subscribers successfully")
}

type Manager struct {
	enabled       bool
	varSubscriber pdvariable.Subscriber

	components     []topology.Component
	subscribers    map[topology.Component]*Subscriber
	topoSubscriber topology.Subscriber
}

func (m *Manager) run() {
	defer func() {
		for _, v := range m.subscribers {
			v.Close()
		}
		m.subscribers = nil
	}()

out:
	for {
		select {
		case vars := <-m.varSubscriber:
			if vars.EnableTopSQL && !m.enabled {
				m.updateSubscribers()
				log.Info("Top SQL is enabled")
			}

			if !vars.EnableTopSQL && m.enabled {
				m.clearSubscribers()
				log.Info("Top SQL is disabled")
			}

			m.enabled = vars.EnableTopSQL
		case coms := <-m.topoSubscriber:
			if len(coms) == 0 {
				log.Warn("got empty subscribers. Seems to be encountering network problems")
				continue
			}

			m.components = coms
			if m.enabled {
				m.updateSubscribers()
			}
		case <-globalStopCh:
			break out
		}
	}
}

func (m *Manager) updateSubscribers() {
	// clean up closed subscribers
	for component, subscriber := range m.subscribers {
		if subscriber.IsDown() {
			subscriber.Close()
			delete(m.subscribers, component)
		}
	}

	in, out := m.getTopoChange()

	// clean up stale subscribers
	for i := range out {
		m.subscribers[out[i]].Close()
		delete(m.subscribers, out[i])
	}

	// set up incoming subscribers
	for i := range in {
		subscriber := NewSubscriber(in[i])
		m.subscribers[in[i]] = subscriber

		scraperWG.Add(1)
		go utils.GoWithRecovery(func() {
			defer scraperWG.Done()
			subscriber.run()
		}, nil)
	}
}

func (m *Manager) getTopoChange() (in, out []topology.Component) {
	curMap := make(map[topology.Component]struct{})

	for i := range m.components {
		component := m.components[i]
		switch component.Name {
		case topology.ComponentTiDB:
		case topology.ComponentTiKV:
		default:
			continue
		}

		curMap[component] = struct{}{}
		if _, contains := m.subscribers[component]; !contains {
			in = append(in, component)
		}
	}

	for c := range m.subscribers {
		if _, contains := curMap[c]; !contains {
			out = append(out, c)
		}
	}

	return
}

func (m *Manager) clearSubscribers() {
	for component, subscriber := range m.subscribers {
		subscriber.Close()
		delete(m.subscribers, component)
	}
}

type Subscriber struct {
	isDown    *atomic.Bool
	component topology.Component
	closeCh   chan struct{}
}

func NewSubscriber(component topology.Component) *Subscriber {
	return &Subscriber{
		isDown:    atomic.NewBool(false),
		component: component,
		closeCh:   make(chan struct{}),
	}
}

func (s *Subscriber) IsDown() bool {
	return s.isDown.Load()
}

func (s *Subscriber) Close() {
	close(s.closeCh)
}

func (s *Subscriber) run() {
	defer s.isDown.Store(true)
	log.Info("starting to scrape top SQL from the component", zap.Any("component", s.component))

	switch s.component.Name {
	case topology.ComponentTiDB:
		s.scrapeTiDB()
	case topology.ComponentTiKV:
		s.scrapeTiKV()
	default:
		log.Error("unexpected scrape target", zap.String("component", s.component.Name))
	}
}

func (s *Subscriber) scrapeTiDB() {
	addr := fmt.Sprintf("%s:%d", s.component.IP, s.component.StatusPort)
	conn, err := dial(addr)
	if err != nil {
		log.Error("failed to dial scrape target", zap.Any("component", s.component), zap.Error(err))
		return
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := tipb.NewTopSQLPubSubClient(conn)
	stream, err := client.Subscribe(ctx, &tipb.TopSQLSubRequest{})
	if err != nil {
		log.Error("failed to call Subscribe", zap.Any("component", s.component), zap.Error(err))
		return
	}

	stopCh := make(chan struct{})
	go utils.GoWithRecovery(func() {
		defer close(stopCh)

		if err := store.Instance(addr, topology.ComponentTiDB); err != nil {
			log.Warn("failed to store instance", zap.Error(err))
			return
		}

		for {
			r, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Warn("failed to receive records from stream", zap.Error(err))
				break
			}

			if record := r.GetRecord(); record != nil {
				err = store.TopSQLRecord(addr, topology.ComponentTiDB, record)
				if err != nil {
					log.Warn("failed to store top SQL records", zap.Error(err))
				}
				continue
			}

			if meta := r.GetSqlMeta(); meta != nil {
				err = store.SQLMeta(meta)
				if err != nil {
					log.Warn("failed to store SQL meta", zap.Error(err))
				}
				continue
			}

			if meta := r.GetPlanMeta(); meta != nil {
				err = store.PlanMeta(meta)
				if err != nil {
					log.Warn("failed to store SQL meta", zap.Error(err))
				}
			}
		}
	}, nil)

	select {
	case <-globalStopCh:
	case <-stopCh:
	case <-s.closeCh:
	}
}

func (s *Subscriber) scrapeTiKV() {
	addr := fmt.Sprintf("%s:%d", s.component.IP, s.component.Port)
	conn, err := dial(addr)
	if err != nil {
		log.Error("failed to dial scrape target", zap.Any("component", s.component), zap.Error(err))
		return
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := resource_usage_agent.NewResourceMeteringPubSubClient(conn)
	records, err := client.Subscribe(ctx, &resource_usage_agent.ResourceMeteringRequest{})
	if err != nil {
		log.Error("failed to call SubCPUTimeRecord", zap.Any("component", s.component), zap.Error(err))
		return
	}

	stopCh := make(chan struct{})
	go utils.GoWithRecovery(func() {
		defer close(stopCh)

		if err := store.Instance(addr, topology.ComponentTiKV); err != nil {
			log.Warn("failed to store instance", zap.Error(err))
			return
		}

		for {
			r, err := records.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Warn("failed to receive records from stream", zap.Error(err))
				break
			}

			err = store.ResourceMeteringRecord(addr, topology.ComponentTiKV, r)
			if err != nil {
				log.Warn("failed to store resource metering records", zap.Error(err))
			}
		}
	}, nil)

	select {
	case <-globalStopCh:
	case <-stopCh:
	case <-s.closeCh:
	}
}

func dial(addr string) (*grpc.ClientConn, error) {
	tlsConfig := config.GetGlobalConfig().Security.GetTLSConfig()

	var tlsOption grpc.DialOption
	if tlsConfig == nil {
		tlsOption = grpc.WithInsecure()
	} else {
		tlsOption = grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
	}

	dialCtx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()

	return grpc.DialContext(
		dialCtx,
		addr,
		tlsOption,
		grpc.WithBlock(),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    10 * time.Second,
			Timeout: 3 * time.Second,
		}),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  100 * time.Millisecond, // Default was 1s.
				Multiplier: 1.6,                    // Default
				Jitter:     0.2,                    // Default
				MaxDelay:   3 * time.Second,        // Default was 120s.
			},
		}),
	)
}
