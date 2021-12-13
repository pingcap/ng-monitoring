package subscriber

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/pingcap/kvproto/pkg/resource_usage_agent"
	"github.com/pingcap/log"
	"github.com/pingcap/ng-monitoring/component/topology"
	"github.com/pingcap/ng-monitoring/component/topsql/store"
	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/ng-monitoring/utils"
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

type Scraper struct {
	ctx     context.Context
	isDown  *atomic.Bool
	closeCh chan struct{}

	component topology.Component

	store store.Store
}

func NewScraper(
	ctx context.Context,
	component topology.Component,
	store store.Store,
) *Scraper {
	return &Scraper{
		ctx:       ctx,
		isDown:    atomic.NewBool(false),
		closeCh:   make(chan struct{}),
		component: component,
		store:     store,
	}
}

func (s *Scraper) IsDown() bool {
	return s.isDown.Load()
}

func (s *Scraper) Close() {
	close(s.closeCh)
}

func (s *Scraper) run() {
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

func (s *Scraper) scrapeTiDB() {
	addr := fmt.Sprintf("%s:%d", s.component.IP, s.component.StatusPort)
	conn, err := dial(addr)
	if err != nil {
		log.Error("failed to dial scrape target", zap.Any("component", s.component), zap.Error(err))
		return
	}
	defer conn.Close()

	client := tipb.NewTopSQLPubSubClient(conn)
	stream, err := client.Subscribe(s.ctx, &tipb.TopSQLSubRequest{})
	if err != nil {
		log.Error("failed to call Subscribe", zap.Any("component", s.component), zap.Error(err))
		return
	}

	stopCh := make(chan struct{})
	go utils.GoWithRecovery(func() {
		defer close(stopCh)

		if err := s.store.Instance(addr, topology.ComponentTiDB); err != nil {
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
				err = s.store.TopSQLRecord(addr, topology.ComponentTiDB, record)
				if err != nil {
					log.Warn("failed to store top SQL records", zap.Error(err))
				}
				continue
			}

			if meta := r.GetSqlMeta(); meta != nil {
				err = s.store.SQLMeta(meta)
				if err != nil {
					log.Warn("failed to store SQL meta", zap.Error(err))
				}
				continue
			}

			if meta := r.GetPlanMeta(); meta != nil {
				err = s.store.PlanMeta(meta)
				if err != nil {
					log.Warn("failed to store SQL meta", zap.Error(err))
				}
			}
		}
	}, nil)

	select {
	case <-s.ctx.Done():
	case <-stopCh:
	case <-s.closeCh:
	}
}

func (s *Scraper) scrapeTiKV() {
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

		if err := s.store.Instance(addr, topology.ComponentTiKV); err != nil {
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

			err = s.store.ResourceMeteringRecord(addr, topology.ComponentTiKV, r)
			if err != nil {
				log.Warn("failed to store resource metering records", zap.Error(err))
			}
		}
	}, nil)

	select {
	case <-s.ctx.Done():
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
