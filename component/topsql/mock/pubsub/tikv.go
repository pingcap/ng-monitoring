package pubsub

import (
	"context"
	"net"
	"time"

	rua "github.com/pingcap/kvproto/pkg/resource_usage_agent"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

var _ rua.ResourceMeteringPubSubServer = &MockTiKVPubSub{}

type tikvStreamAccessor = func(_ rua.ResourceMeteringPubSub_SubscribeServer) error

type MockTiKVPubSub struct {
	ctx    context.Context
	cancel context.CancelFunc

	listener net.Listener
	server   *grpc.Server

	accessor chan tikvStreamAccessor
}

func NewMockTiKVPubSub() *MockTiKVPubSub {
	ctx, cancel := context.WithCancel(context.Background())

	return &MockTiKVPubSub{
		ctx:      ctx,
		cancel:   cancel,
		accessor: make(chan tikvStreamAccessor),
	}
}

func (s *MockTiKVPubSub) Listen() (ip string, port uint, err error) {
	s.listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return
	}

	s.server = grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    10 * time.Second,
			Timeout: 3 * time.Second,
		}),
	)
	rua.RegisterResourceMeteringPubSubServer(s.server, s)

	addr := s.listener.Addr().(*net.TCPAddr)
	return addr.IP.String(), uint(addr.Port), nil
}

func (s *MockTiKVPubSub) Serve() error {

	return s.server.Serve(s.listener)
}

func (s *MockTiKVPubSub) AccessStream(fn func(_ rua.ResourceMeteringPubSub_SubscribeServer) error) {
	s.accessor <- fn
}

func (s *MockTiKVPubSub) Stop() {
	s.server.Stop()
	s.cancel()
}

func (s *MockTiKVPubSub) Subscribe(
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
