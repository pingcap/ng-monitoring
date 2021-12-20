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
	ctx       context.Context
	cancel    context.CancelFunc
	tlsConfig *tls.Config
	component topology.Component
	store     store.Store
}

func NewScraper(ctx context.Context, component topology.Component, store store.Store, tlsConfig *tls.Config) *Scraper {
	ctx, cancel := context.WithCancel(ctx)

	return &Scraper{
		ctx:       ctx,
		cancel:    cancel,
		tlsConfig: tlsConfig,
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
	bo := newBackoffScrape(s.ctx, s.tlsConfig, addr, s.component)
	defer bo.close()

	stream := bo.reconnectTiDB()
	if stream == nil {
		return
	}

	if err := s.store.Instance(addr, topology.ComponentTiDB); err != nil {
		log.Warn("failed to store instance", zap.Error(err))
		return
	}

	for {
		r, err := stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			log.Warn("failed to receive records from stream", zap.Error(err))
			stream = bo.reconnectTiDB()
			if stream == nil {
				return
			}
			continue
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
	bo := newBackoffScrape(s.ctx, s.tlsConfig, addr, s.component)
	defer bo.close()

	stream := bo.reconnectTiKV()
	if stream == nil {
		return
	}

	if err := s.store.Instance(addr, topology.ComponentTiKV); err != nil {
		log.Warn("failed to store instance", zap.Error(err))
		return
	}

	for {
		r, err := stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			log.Warn("failed to receive records from stream", zap.Error(err))
			stream = bo.reconnectTiKV()
			if stream == nil {
				return
			}
			continue
		}

		err = s.store.ResourceMeteringRecord(addr, topology.ComponentTiKV, r)
		if err != nil {
			log.Warn("failed to store resource metering records", zap.Error(err))
		}
	}

}

func dial(ctx context.Context, tlsConfig *tls.Config, addr string) (*grpc.ClientConn, error) {
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

type backoffScrape struct {
	ctx       context.Context
	tlsCfg    *tls.Config
	address   string
	component topology.Component

	conn   *grpc.ClientConn
	client interface{}
	stream interface{}

	retryTimes    uint
	maxRetryTimes uint
}

func newBackoffScrape(ctx context.Context, tlsCfg *tls.Config, address string, component topology.Component) *backoffScrape {
	return &backoffScrape{
		ctx:       ctx,
		tlsCfg:    tlsCfg,
		address:   address,
		component: component,

		retryTimes:    0,
		maxRetryTimes: 8,
	}
}

func (bo *backoffScrape) reconnect() {
	for {
		if bo.conn != nil {
			_ = bo.conn.Close()
			bo.conn = nil
			bo.client = nil
			bo.stream = nil
		}

		select {
		case <-bo.ctx.Done():
			return
		default:
		}

		if bo.retryTimes > bo.maxRetryTimes {
			log.Warn("retry to scrape component too many times, stop", zap.Any("component", bo.component), zap.Uint("retried", bo.retryTimes))
			return
		}

		if bo.retryTimes > 0 {
			time.Sleep(time.Second * (2 << bo.retryTimes))
			log.Warn("retry to scrape component", zap.Any("component", bo.component), zap.Uint("retried", bo.retryTimes))
		}

		conn, err := dial(bo.ctx, bo.tlsCfg, bo.address)
		if err != nil {
			log.Warn("failed to dial scrape target", zap.Any("component", bo.component), zap.Error(err))
			bo.retryTimes += 1
			continue
		}

		bo.conn = conn
		switch bo.component.Name {
		case "tidb":
			client := tipb.NewTopSQLPubSubClient(conn)
			bo.client = client
			if bo.stream, err = client.Subscribe(bo.ctx, &tipb.TopSQLSubRequest{}); err != nil {
				log.Warn("failed to call Subscribe", zap.Any("component", bo.component), zap.Error(err))
				bo.retryTimes += 1
				continue
			}
		case "tikv":
			client := resource_usage_agent.NewResourceMeteringPubSubClient(conn)
			bo.client = client
			if bo.stream, err = client.Subscribe(bo.ctx, &resource_usage_agent.ResourceMeteringRequest{}); err != nil {
				log.Warn("failed to call Subscribe", zap.Any("component", bo.component), zap.Error(err))
				bo.retryTimes += 1
				continue
			}
		default:
			break
		}

		bo.retryTimes = 0
		return
	}
}

func (bo *backoffScrape) reconnectTiDB() tipb.TopSQLPubSub_SubscribeClient {
	bo.reconnect()
	if bo.stream == nil {
		return nil
	}
	if stream, ok := bo.stream.(tipb.TopSQLPubSub_SubscribeClient); ok {
		return stream
	}
	return nil
}

func (bo *backoffScrape) reconnectTiKV() resource_usage_agent.ResourceMeteringPubSub_SubscribeClient {
	bo.reconnect()
	if bo.stream == nil {
		return nil
	}
	if stream, ok := bo.stream.(resource_usage_agent.ResourceMeteringPubSub_SubscribeClient); ok {
		return stream
	}
	return nil
}

func (bo *backoffScrape) close() {
	if bo.conn != nil {
		_ = bo.conn.Close()
		bo.conn = nil
		bo.client = nil
		bo.stream = nil
	}
}
