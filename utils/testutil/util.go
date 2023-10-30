package testutil

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
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
	badger.DefaultIteratorOptions.PrefetchValues = false

	opts := badger.DefaultOptions(storagePath).
		WithZSTDCompressionLevel(3).
		WithBlockSize(8 * 1024).
		WithValueThreshold(128 * 1024).
		WithValueLogFileSize(64 * 1024 * 1024).
		WithBlockCacheSize(16 * 1024 * 1024).
		WithMemTableSize(16 * 1024 * 1024)

	engine, err := badgerengine.NewEngine(opts)
	require.NoError(t, err)
	db, err := genji.New(context.Background(), engine)
	require.NoError(t, err)
	return db
}

func NewBadgerDB(t *testing.T, storagePath string) *badger.DB {
	badger.DefaultIteratorOptions.PrefetchValues = false

	opts := badger.DefaultOptions(storagePath).
		WithZSTDCompressionLevel(3).
		WithBlockSize(8 * 1024).
		WithValueThreshold(128 * 1024).
		WithValueLogFileSize(64 * 1024 * 1024).
		WithBlockCacheSize(16 * 1024 * 1024).
		WithMemTableSize(16 * 1024 * 1024)

	engine, err := badgerengine.NewEngine(opts)
	require.NoError(t, err)
	return engine.DB
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
		_, err := writer.Write([]byte("profile"))
		require.NoError(t, err)
	})

	router.HandleFunc("/debug/pprof/mutex", func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
		_, err := writer.Write([]byte("mutex"))
		require.NoError(t, err)
	})

	router.HandleFunc("/debug/pprof/goroutine", func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
		_, err := writer.Write([]byte("goroutine"))
		require.NoError(t, err)
	})

	router.HandleFunc("/debug/pprof/heap", func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
		// jemalloc heap profile format
		_, err := writer.Write([]byte("heap_v2/524288"))
		require.NoError(t, err)
	})

	router.HandleFunc("/debug/pprof/symbol", func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
		if request.Method == http.MethodGet {
			_, err := writer.Write([]byte("num_symbols: 1\n"))
			require.NoError(t, err)
		}
	})

	router.HandleFunc("/debug/pprof/cmdline", func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
		_, err := writer.Write([]byte("cmdline"))
		require.NoError(t, err)
	})

	httpServer := &http.Server{
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
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
			Members: []pdclient.GetMembersResponseMember{
				{GitHash: "abcd", ClientUrls: []string{"http://" + s.Addr}, DeployPath: "data", BinaryVersion: "v5.3.0", MemberID: 1},
			},
		}
		s.writeJson(t, writer, resp)
	})

	router.HandleFunc("/pd/api/v1/stores", func(writer http.ResponseWriter, request *http.Request) {
		resp := pdclient.GetStoresResponse{
			Stores: []pdclient.GetStoresResponseStoresElem{
				{pdclient.GetStoresResponseStore{Address: "127.0.0.1:20160", ID: 1, Version: "v5.3.0", StatusAddress: "127.0.0.1:20180", StartTimestamp: time.Now().Unix(), StateName: "up"}},
			},
		}
		s.writeJson(t, writer, resp)
	})

	httpServer := &http.Server{
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
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

func SetupCert() (serverTLSConf *tls.Config, clientTLSConf *tls.Config, err error) {
	// set up our CA certificate
	ca := &x509.Certificate{
		SerialNumber: big.NewInt(2019),
		Subject: pkix.Name{
			Organization:  []string{"Company, INC."},
			Country:       []string{"US"},
			Province:      []string{""},
			Locality:      []string{"San Francisco"},
			StreetAddress: []string{"Golden Gate Bridge"},
			PostalCode:    []string{"94016"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(10, 0, 0),
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}

	// create our private and public key
	caPrivKey, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return nil, nil, err
	}

	// create the CA
	caBytes, err := x509.CreateCertificate(rand.Reader, ca, ca, &caPrivKey.PublicKey, caPrivKey)
	if err != nil {
		return nil, nil, err
	}

	// pem encode
	caPEM := new(bytes.Buffer)
	if err = pem.Encode(caPEM, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: caBytes,
	}); err != nil {
		return nil, nil, err
	}

	caPrivKeyPEM := new(bytes.Buffer)
	if err = pem.Encode(caPrivKeyPEM, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(caPrivKey),
	}); err != nil {
		return nil, nil, err
	}

	// set up our server certificate
	cert := &x509.Certificate{
		SerialNumber: big.NewInt(2021),
		Subject: pkix.Name{
			Organization:  []string{"Company, INC."},
			Country:       []string{"US"},
			Province:      []string{""},
			Locality:      []string{"San Francisco"},
			StreetAddress: []string{"Golden Gate Bridge"},
			PostalCode:    []string{"94016"},
		},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().AddDate(10, 0, 0),
		SubjectKeyId: []byte{1, 2, 3, 4, 6},
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}

	certPrivKey, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return nil, nil, err
	}

	certBytes, err := x509.CreateCertificate(rand.Reader, cert, ca, &certPrivKey.PublicKey, caPrivKey)
	if err != nil {
		return nil, nil, err
	}

	certPEM := new(bytes.Buffer)
	if err = pem.Encode(certPEM, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certBytes,
	}); err != nil {
		return nil, nil, err
	}

	certPrivKeyPEM := new(bytes.Buffer)
	if err = pem.Encode(certPrivKeyPEM, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(certPrivKey),
	}); err != nil {
		return nil, nil, err
	}

	serverCert, err := tls.X509KeyPair(certPEM.Bytes(), certPrivKeyPEM.Bytes())
	if err != nil {
		return nil, nil, err
	}

	serverTLSConf = &tls.Config{ // nolint:gosec
		Certificates: []tls.Certificate{serverCert},
	}

	certpool := x509.NewCertPool()
	certpool.AppendCertsFromPEM(caPEM.Bytes())
	clientTLSConf = &tls.Config{ // nolint:gosec
		RootCAs: certpool,
	}

	return
}
