package testutil

import (
	"context"
	"net"
	"net/http"
	"strconv"
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/genjidb/genji"
	"github.com/genjidb/genji/engine/badgerengine"
	"github.com/stretchr/testify/require"
)

func NewGenjiDB(t *testing.T, storagePath string) *genji.DB {
	opts := badger.DefaultOptions(storagePath).
		WithZSTDCompressionLevel(3).
		WithBlockSize(8 * 1024).
		WithValueThreshold(128 * 1024)

	engine, err := badgerengine.NewEngine(opts)
	require.NoError(t, err)
	db, err := genji.New(context.Background(), engine)
	require.NoError(t, err)
	return db
}

type MockProfileServer struct {
	Addr       string
	Port       uint
	HttpServer *http.Server
}

func CreateMockProfileServer(t *testing.T) *MockProfileServer {
	s := &MockProfileServer{}
	s.Start(t)
	return s
}

func (s *MockProfileServer) Start(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	router := http.NewServeMux()
	router.HandleFunc("/debug/pprof/profile", func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
		writer.Write([]byte("profile"))
	})

	router.HandleFunc("/debug/pprof/mutex", func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
		writer.Write([]byte("mutex"))
	})

	router.HandleFunc("/debug/pprof/goroutine", func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
		writer.Write([]byte("goroutine"))
	})

	router.HandleFunc("/debug/pprof/heap", func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
		writer.Write([]byte("heap"))
	})

	httpServer := &http.Server{
		Handler: router,
	}
	go func() {
		if err = httpServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			require.NoError(t, err)
		}
	}()
	var port string
	s.Addr, port, err = net.SplitHostPort(listener.Addr().String())
	require.NoError(t, err)
	p, err := strconv.ParseUint(port, 10, 64)
	require.NoError(t, err)
	s.Port = uint(p)
	s.HttpServer = httpServer
}

func (s *MockProfileServer) Stop(t *testing.T) {
	err := s.HttpServer.Close()
	require.NoError(t, err)
}
