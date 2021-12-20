package pubsub

import (
	"context"
	"net"
	"time"

	"github.com/pingcap/tipb/go-tipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

var _ tipb.TopSQLPubSubServer = &MockTiDBPubSub{}

type tidbStreamAccessor = func(_ tipb.TopSQLPubSub_SubscribeServer) error

type MockTiDBPubSub struct {
	ctx    context.Context
	cancel context.CancelFunc

	listener net.Listener
	server   *grpc.Server

	accessor chan tidbStreamAccessor
}

func NewMockTiDBPubSub() *MockTiDBPubSub {
	ctx, cancel := context.WithCancel(context.Background())

	return &MockTiDBPubSub{
		ctx:      ctx,
		cancel:   cancel,
		accessor: make(chan tidbStreamAccessor),
	}
}

func (s *MockTiDBPubSub) Listen() (ip string, port uint, err error) {
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
	tipb.RegisterTopSQLPubSubServer(s.server, s)

	addr := s.listener.Addr().(*net.TCPAddr)
	return addr.IP.String(), uint(addr.Port), nil
}

func (s *MockTiDBPubSub) Serve() error {
	return s.server.Serve(s.listener)
}

func (s *MockTiDBPubSub) AccessStream(fn func(_ tipb.TopSQLPubSub_SubscribeServer) error) {
	s.accessor <- fn
}

func (s *MockTiDBPubSub) Stop() {
	s.server.Stop()
	s.cancel()
}

func (s *MockTiDBPubSub) Subscribe(
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
