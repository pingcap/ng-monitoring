package tests

import (
	"net"
	"net/http"
	"sync"
	"time"

	rua "github.com/pingcap/kvproto/pkg/resource_usage_agent"
	"github.com/pingcap/tipb/go-tipb"
	"google.golang.org/grpc"
)

var _ tipb.TopSQLPubSubServer = &MockTiDBServer{}

type MockTiDBServer struct {
	listener net.Listener
	server   *grpc.Server
	records  []tipb.TopSQLRecord
	changed  bool
	mu       sync.Mutex
}

func NewMockTiDBServer() *MockTiDBServer {
	return &MockTiDBServer{}
}

func (s *MockTiDBServer) PushRecords(records []tipb.TopSQLRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, records...)
	s.changed = true
}

func (s *MockTiDBServer) Listen() (addr string, err error) {
	s.listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return
	}
	return s.listener.Addr().String(), nil
}

func (s *MockTiDBServer) Serve() error {
	s.server = grpc.NewServer()
	tipb.RegisterTopSQLPubSubServer(s.server, s)
	return s.server.Serve(s.listener)
}

func (s *MockTiDBServer) Stop() {
	s.server.Stop()
}

func (s *MockTiDBServer) Subscribe(req *tipb.TopSQLSubRequest, stream tipb.TopSQLPubSub_SubscribeServer) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case <-ticker.C:
			s.mu.Lock()
			records := s.records
			changed := s.changed
			s.records = nil
			s.mu.Unlock()
			if !changed {
				continue
			}
			for i := range records {
				record := &records[i]
				if err := stream.Send(&tipb.TopSQLSubResponse{
					RespOneof: &tipb.TopSQLSubResponse_Record{
						Record: record,
					},
				}); err != nil {
					panic(err)
				}
			}
		}
	}
}

var _ rua.ResourceMeteringPubSubServer = &MockTiKVServer{}

type MockTiKVServer struct {
	listener net.Listener
	server   *grpc.Server
	records  []*rua.ResourceUsageRecord
	changed  bool
	mu       sync.Mutex
}

func NewMockTiKVServer() *MockTiKVServer {
	return &MockTiKVServer{}
}

func (s *MockTiKVServer) PushRecords(records []*rua.ResourceUsageRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, records...)
	s.changed = true
}

func (s *MockTiKVServer) Listen() (addr string, err error) {
	s.listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return
	}
	return s.listener.Addr().String(), nil
}

func (s *MockTiKVServer) Serve() error {
	s.server = grpc.NewServer()
	rua.RegisterResourceMeteringPubSubServer(s.server, s)
	return s.server.Serve(s.listener)
}

func (s *MockTiKVServer) Stop() {
	s.server.Stop()
}

func (s *MockTiKVServer) Subscribe(req *rua.ResourceMeteringRequest, stream rua.ResourceMeteringPubSub_SubscribeServer) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case <-ticker.C:
			s.mu.Lock()
			records := s.records
			changed := s.changed
			s.records = nil
			s.mu.Unlock()
			if !changed {
				continue
			}
			for _, record := range records {
				if err := stream.Send(record); err != nil {
					panic(err)
				}
			}
		}
	}
}

var _ http.ResponseWriter = &MockResponseWriter{}

type MockResponseWriter struct {
	StatusCode int
	Body       []byte
	header     http.Header
}

func NewMockResponseWriter() *MockResponseWriter {
	return &MockResponseWriter{
		header: map[string][]string{},
	}
}

func (w *MockResponseWriter) Header() http.Header {
	return w.header
}

func (w *MockResponseWriter) Write(buf []byte) (int, error) {
	w.Body = buf
	return len(buf), nil
}

func (w *MockResponseWriter) WriteHeader(statusCode int) {
	w.StatusCode = statusCode
}
