package mock

import (
	"context"
	"crypto/tls"
	"net"
	"time"

	rua "github.com/pingcap/kvproto/pkg/resource_usage_agent"
	"github.com/pingcap/tipb/go-tipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

type tidbStreamAccessor = func(_ tipb.TopSQLPubSub_SubscribeServer) error
type tikvStreamAccessor = func(_ rua.ResourceMeteringPubSub_SubscribeServer) error

type MockPubSub struct {
	ctx    context.Context
	cancel context.CancelFunc

	listener net.Listener
	server   *grpc.Server

	tidbAccessor chan tidbStreamAccessor
	tikvAccessor chan tikvStreamAccessor
}

func NewMockPubSub() *MockPubSub {
	ctx, cancel := context.WithCancel(context.Background())

	return &MockPubSub{
		ctx:          ctx,
		cancel:       cancel,
		tidbAccessor: make(chan tidbStreamAccessor),
		tikvAccessor: make(chan tikvStreamAccessor),
	}
}

func (s *MockPubSub) Listen(addr string, tls *tls.Config) (ip string, port uint, err error) {
	s.listener, err = net.Listen("tcp", addr)
	if err != nil {
		return
	}

	var opts []grpc.ServerOption
	if tls != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(tls)))
	}

	opts = append(opts, grpc.KeepaliveParams(keepalive.ServerParameters{
		Time:    10 * time.Second,
		Timeout: 3 * time.Second,
	}))

	s.server = grpc.NewServer(opts...)
	tipb.RegisterTopSQLPubSubServer(s.server, &tidbService{
		ctx:      s.ctx,
		accessor: s.tidbAccessor,
	})
	rua.RegisterResourceMeteringPubSubServer(s.server, &tikvService{
		ctx:      s.ctx,
		accessor: s.tikvAccessor,
	})

	adr := s.listener.Addr().(*net.TCPAddr)
	return adr.IP.String(), uint(adr.Port), nil
}

func (s *MockPubSub) Serve() error {
	return s.server.Serve(s.listener)
}

func (s *MockPubSub) AccessTiDBStream(fn func(_ tipb.TopSQLPubSub_SubscribeServer) error) {
	s.tidbAccessor <- fn
}

func (s *MockPubSub) AccessTiKVStream(fn func(_ rua.ResourceMeteringPubSub_SubscribeServer) error) {
	s.tikvAccessor <- fn
}

func (s *MockPubSub) Stop() {
	s.server.Stop()
	s.cancel()
}

type tidbService struct {
	ctx      context.Context
	accessor chan tidbStreamAccessor
}

var _ tipb.TopSQLPubSubServer = &tidbService{}

func (s *tidbService) Subscribe(
	_ *tipb.TopSQLSubRequest,
	stream tipb.TopSQLPubSub_SubscribeServer,
) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case <-s.ctx.Done():
			return nil
		case accessor := <-s.accessor:
			if err := accessor(stream); err != nil {
				return err
			}
		}
	}
}

type tikvService struct {
	ctx      context.Context
	accessor chan tikvStreamAccessor
}

var _ rua.ResourceMeteringPubSubServer = &tikvService{}

func (s *tikvService) Subscribe(
	_ *rua.ResourceMeteringRequest,
	stream rua.ResourceMeteringPubSub_SubscribeServer,
) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case <-s.ctx.Done():
			return nil
		case accessor := <-s.accessor:
			if err := accessor(stream); err != nil {
				return err
			}
		}
	}
}
