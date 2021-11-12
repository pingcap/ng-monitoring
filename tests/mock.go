package tests

import (
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	rua "github.com/pingcap/kvproto/pkg/resource_usage_agent"
	"google.golang.org/grpc"
)

var _ rua.ResourceMeteringPubSubServer = &MockTiKVServer{}

type MockTiKVServer struct {
	server  *grpc.Server
	records []*rua.ResourceUsageRecord
	changed bool
	mu      sync.Mutex
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

func (s *MockTiKVServer) Serve(port int) error {
	listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return err
	}
	s.server = grpc.NewServer()
	rua.RegisterResourceMeteringPubSubServer(s.server, s)
	return s.server.Serve(listener)
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
			if records != nil {
				for _, record := range records {
					if err := stream.Send(record); err != nil {
						panic(err)
					}
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
