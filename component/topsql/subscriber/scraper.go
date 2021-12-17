package subscriber

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"time"

	"github.com/pingcap/kvproto/pkg/resource_usage_agent"
	"github.com/pingcap/log"
	"github.com/pingcap/ng-monitoring/component/topology"
	"github.com/pingcap/ng-monitoring/component/topsql/store"
	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/tipb/go-tipb"
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
	ctx    context.Context
	cancel context.CancelFunc

	component topology.Component
	store     store.Store
}

func NewScraper(
	ctx context.Context,
	component topology.Component,
	store store.Store,
) *Scraper {
	ctx, cancel := context.WithCancel(ctx)

	return &Scraper{
		ctx:       ctx,
		cancel:    cancel,
		component: component,
		store:     store,
	}
}

func (s *Scraper) IsDown() bool {
	select {
	case <-s.ctx.Done():
		return true
	default:
		return false
	}
}

func (s *Scraper) Close() {
	s.cancel()
}

func (s *Scraper) Run() {
	defer s.cancel()
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
	conn, err := dial(s.ctx, addr)
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
}

func (s *Scraper) scrapeTiKV() {
	addr := fmt.Sprintf("%s:%d", s.component.IP, s.component.Port)
	conn, err := dial(s.ctx, addr)
	if err != nil {
		log.Error("failed to dial scrape target", zap.Any("component", s.component), zap.Error(err))
		return
	}
	defer conn.Close()

	client := resource_usage_agent.NewResourceMeteringPubSubClient(conn)
	records, err := client.Subscribe(s.ctx, &resource_usage_agent.ResourceMeteringRequest{})
	if err != nil {
		log.Error("failed to call SubCPUTimeRecord", zap.Any("component", s.component), zap.Error(err))
		return
	}

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

}

func dial(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	var tlsConfig *tls.Config
	if globalConfig := config.GetGlobalConfig(); globalConfig != nil {
		tlsConfig = globalConfig.Security.GetTLSConfig()
	}

	var tlsOption grpc.DialOption
	if tlsConfig == nil {
		tlsOption = grpc.WithInsecure()
	} else {
		tlsOption = grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
	}

	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
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
