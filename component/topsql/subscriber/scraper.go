package subscriber

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/ng-monitoring/component/subscriber"
	"github.com/pingcap/ng-monitoring/component/topology"
	"github.com/pingcap/ng-monitoring/component/topsql/store"
	"github.com/pingcap/ng-monitoring/utils"

	"github.com/pingcap/kvproto/pkg/resource_usage_agent"
	"github.com/pingcap/log"
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
	// schemaInfo is used to store the schema information. tableID -> schema
	schemaInfo *sync.Map
}

func NewScraper(ctx context.Context, schemaInfo *sync.Map, component topology.Component, store store.Store, tlsConfig *tls.Config) *Scraper {
	switch component.Name {
	case topology.ComponentTiDB, topology.ComponentTiKV:
		ctx, cancel := context.WithCancel(ctx)
		return &Scraper{
			ctx:        ctx,
			cancel:     cancel,
			tlsConfig:  tlsConfig,
			component:  component,
			store:      store,
			schemaInfo: schemaInfo,
		}
	default:
		return nil
	}
}

var _ subscriber.Scraper = &Scraper{}

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
	log.Info("starting to scrape Top SQL from the component", zap.Any("component", s.component))
	defer func() {
		s.cancel()
		log.Info("stop scraping Top SQL from the component", zap.Any("component", s.component))
	}()

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

	for {
		record := bo.scrapeTiDBRecord()
		if record == nil {
			return
		}

		if r := record.GetRecord(); r != nil {
			err := s.store.TopSQLRecord(addr, topology.ComponentTiDB, r)
			if err != nil {
				log.Warn("failed to store Top SQL records", zap.Error(err))
			}
			continue
		}

		if meta := record.GetSqlMeta(); meta != nil {
			err := s.store.SQLMeta(meta)
			if err != nil {
				log.Warn("failed to store SQL meta", zap.Error(err))
			}
			continue
		}

		if meta := record.GetPlanMeta(); meta != nil {
			err := s.store.PlanMeta(meta)
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

	for {
		record := bo.scrapeTiKVRecord()
		if record == nil {
			return
		}

		err := s.store.ResourceMeteringRecord(addr, topology.ComponentTiKV, record, s.schemaInfo)
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

	firstWaitTime time.Duration
	maxRetryTimes uint
}

func newBackoffScrape(ctx context.Context, tlsCfg *tls.Config, address string, component topology.Component) *backoffScrape {
	return &backoffScrape{
		ctx:       ctx,
		tlsCfg:    tlsCfg,
		address:   address,
		component: component,

		firstWaitTime: 2 * time.Second,
		maxRetryTimes: 8,
	}
}

func (bo *backoffScrape) scrapeTiDBRecord() *tipb.TopSQLSubResponse {
	if record := bo.scrape(); record != nil {
		if res, ok := record.(*tipb.TopSQLSubResponse); ok {
			return res
		}
	}

	return nil
}

func (bo *backoffScrape) scrapeTiKVRecord() *resource_usage_agent.ResourceUsageRecord {
	if record := bo.scrape(); record != nil {
		if res, ok := record.(*resource_usage_agent.ResourceUsageRecord); ok {
			return res
		}
	}

	return nil
}

func (bo *backoffScrape) scrape() interface{} {
	if bo.stream != nil {
		switch s := bo.stream.(type) {
		case tipb.TopSQLPubSub_SubscribeClient:
			if record, _ := s.Recv(); record != nil {
				return record
			}
		case resource_usage_agent.ResourceMeteringPubSub_SubscribeClient:
			if record, _ := s.Recv(); record != nil {
				return record
			}
		}
	}

	return bo.backoffScrape()
}

func (bo *backoffScrape) backoffScrape() (record interface{}) {
	utils.WithRetryBackoff(bo.ctx, bo.maxRetryTimes, bo.firstWaitTime, func(retried uint) bool {
		if retried != 0 {
			log.Warn("retry to scrape component", zap.Any("component", bo.component), zap.Uint("retried", retried))
		}

		if bo.conn != nil {
			_ = bo.conn.Close()
			bo.conn = nil
			bo.client = nil
			bo.stream = nil
		}

		conn, err := dial(bo.ctx, bo.tlsCfg, bo.address)
		if err != nil {
			log.Warn("failed to dial scrape target", zap.Any("component", bo.component), zap.Error(err))
			return false
		}

		bo.conn = conn
		switch bo.component.Name {
		case topology.ComponentTiDB:
			client := tipb.NewTopSQLPubSubClient(conn)
			bo.client = client
			stream, err := client.Subscribe(bo.ctx, &tipb.TopSQLSubRequest{})
			if err != nil {
				log.Warn("failed to call Subscribe", zap.Any("component", bo.component), zap.Error(err))
				return false
			}
			bo.stream = stream
			record, err = stream.Recv()
			if err != nil {
				log.Warn("failed to call Subscribe", zap.Any("component", bo.component), zap.Error(err))
				return false
			}

			return true

		case topology.ComponentTiKV:
			client := resource_usage_agent.NewResourceMeteringPubSubClient(conn)
			bo.client = client
			stream, err := client.Subscribe(bo.ctx, &resource_usage_agent.ResourceMeteringRequest{})
			if err != nil {
				log.Warn("failed to call Subscribe", zap.Any("component", bo.component), zap.Error(err))
				return false
			}
			bo.stream = stream
			record, err = stream.Recv()
			if err != nil {
				log.Warn("failed to call Subscribe", zap.Any("component", bo.component), zap.Error(err))
				return false
			}

			return true
		default:
			return true
		}
	})

	return
}

func (bo *backoffScrape) close() {
	if bo.conn != nil {
		_ = bo.conn.Close()
		bo.conn = nil
		bo.client = nil
		bo.stream = nil
	}
}
