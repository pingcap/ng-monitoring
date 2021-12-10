package testutil

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/genjidb/genji"
	"github.com/genjidb/genji/engine/badgerengine"
	"github.com/pingcap/tidb-dashboard/util/client/pdclient"
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

type MockPDHTTPServer struct {
	Addr       string
	Health     bool
	httpServer *http.Server
}

func (s *MockPDHTTPServer) Setup(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	router := http.NewServeMux()
	router.HandleFunc("/pd/api/v1/health", func(writer http.ResponseWriter, request *http.Request) {
		if !s.Health {
			writer.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		resp := pdclient.GetHealthResponse{
			{MemberID: 1, Health: s.Health},
		}
		s.writeJson(t, writer, resp)
	})

	router.HandleFunc("/pd/api/v1/status", func(writer http.ResponseWriter, request *http.Request) {
		resp := pdclient.GetStatusResponse{
			StartTimestamp: time.Now().Unix(),
		}
		s.writeJson(t, writer, resp)
	})

	router.HandleFunc("/pd/api/v1/members", func(writer http.ResponseWriter, request *http.Request) {
		resp := pdclient.GetMembersResponse{
			Members: []struct {
				GitHash       string   `json:"git_hash"`
				ClientUrls    []string `json:"client_urls"`
				DeployPath    string   `json:"deploy_path"`
				BinaryVersion string   `json:"binary_version"`
				MemberID      uint64   `json:"member_id"`
			}{
				{GitHash: "abcd", ClientUrls: []string{"http://" + s.Addr}, DeployPath: "data", BinaryVersion: "v5.3.0", MemberID: 1},
			},
		}
		s.writeJson(t, writer, resp)
	})

	router.HandleFunc("/pd/api/v1/stores", func(writer http.ResponseWriter, request *http.Request) {
		resp := pdclient.GetStoresResponse{
			Stores: []struct {
				Store pdclient.GetStoresResponseStore
			}{
				{pdclient.GetStoresResponseStore{Address: "127.0.0.1:20160", ID: 1, Version: "v5.3.0", StatusAddress: "127.0.0.1:20180", StartTimestamp: time.Now().Unix(), StateName: "up"}},
			},
		}
		s.writeJson(t, writer, resp)
	})

	httpServer := &http.Server{
		Handler: router,
	}
	go func() {
		if err = httpServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			require.NoError(t, err)
		}
	}()
	s.Health = true
	s.Addr = listener.Addr().String()
	s.httpServer = httpServer
}

func (s *MockPDHTTPServer) writeJson(t *testing.T, writer http.ResponseWriter, resp interface{}) {
	writer.WriteHeader(http.StatusOK)
	data, err := json.Marshal(resp)
	require.NoError(t, err)
	_, err = writer.Write(data)
	require.NoError(t, err)
}

func (s *MockPDHTTPServer) Close(t *testing.T) {
	err := s.httpServer.Close()
	require.NoError(t, err)
}
