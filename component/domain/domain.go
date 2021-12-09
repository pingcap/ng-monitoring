package domain

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ng_monitoring/config"
	"github.com/pingcap/ng_monitoring/utils"
	"github.com/pingcap/tidb-dashboard/util/client/httpclient"
	"github.com/pingcap/tidb-dashboard/util/client/pdclient"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

const (
	minRetryInterval = time.Millisecond * 10
	maxRetryInterval = time.Second
)

var ErrDomainClosed = errors.New("domain closed")

type Domain struct {
	pdCli       atomic.Value // *pdclient.APIClient
	etcdCli     atomic.Value // *clientv3.Client
	cfgChangeCh chan struct{}
	initialized chan struct{}
	closed      chan struct{}
}

func NewDomain() *Domain {
	cm := &Domain{
		cfgChangeCh: config.SubscribeConfigChange(),
		initialized: make(chan struct{}),
		closed:      make(chan struct{}),
	}
	go utils.GoWithRecovery(cm.start, nil)
	return cm
}

// WARN: call this function will blocked until successfully created PD client.
func (cm *Domain) GetPDClient() (*pdclient.APIClient, error) {
	cm.waitUntilInitialized()
	if cm.isClose() {
		return nil, ErrDomainClosed
	}
	cli := cm.pdCli.Load()
	if cli == nil {
		return nil, ErrDomainClosed
	}
	return cli.(*pdclient.APIClient), nil
}

// WARN: call this function will blocked until successfully created etcd client.
func (cm *Domain) GetEtcdClient() (*clientv3.Client, error) {
	cm.waitUntilInitialized()
	if cm.isClose() {
		return nil, ErrDomainClosed
	}
	cli := cm.etcdCli.Load()
	if cli == nil {
		log.Fatal("should never happen")
	}
	return cli.(*clientv3.Client), nil
}

func (cm *Domain) waitUntilInitialized() {
	<-cm.initialized
}

func (cm *Domain) initialize() {
	cm.createClientWithRetry()
	close(cm.initialized)
}

func (cm *Domain) createClientWithRetry() {
	latest := time.Now()
	backoff := minRetryInterval
	for {
		select {
		case <-cm.closed:
			return
		default:
		}

		cfg := config.GetGlobalConfig()
		pdCli, etcdCli, err := createClient(cfg)
		if err == nil {
			cm.pdCli.Store(pdCli)
			cm.etcdCli.Store(etcdCli)
			return
		}
		if time.Since(latest) > time.Second*5 {
			latest = time.Now()
			log.Warn("create pd/etcd client failed", zap.Error(err))
		}
		time.Sleep(backoff)
		backoff = backoff * 2
		if backoff > maxRetryInterval {
			backoff = maxRetryInterval
		}
	}
}

func (cm *Domain) start() {
	cm.initialize()
	for {
		select {
		case <-cm.closed:
			return
		case <-cm.cfgChangeCh:
			cm.createClientWithRetry()
		}
	}
}

func (cm *Domain) Close() {
	if cm.isClose() {
		return
	}
	close(cm.closed)
}

func (cm *Domain) isClose() bool {
	select {
	case <-cm.closed:
		return true
	default:
		return false
	}
}

func NewClientForTest(cfg *config.Config, etcdCli *clientv3.Client) (*Domain, error) {
	pdCli, err := CreatePDClient(cfg)
	if err != nil {
		return nil, err
	}
	cm := &Domain{}
	cm.pdCli.Store(pdCli)
	cm.etcdCli.Store(etcdCli)
	close(cm.initialized)
	return cm, nil
}

func createClient(cfg *config.Config) (*pdclient.APIClient, *clientv3.Client, error) {
	if len(cfg.PD.Endpoints) == 0 {
		return nil, nil, fmt.Errorf("unexpected empty pd endpoints, please specify at least one pd endpoint")
	}
	etcdCli, err := pdclient.NewEtcdClient(pdclient.EtcdClientConfig{
		Endpoints: cfg.PD.Endpoints,
		Context:   context.Background(),
		TLS:       cfg.Security.GetTLSConfig(),
	})
	if err != nil {
		return nil, nil, err
	}

	pdCli, err := CreatePDClient(cfg)
	if err != nil {
		return nil, nil, err
	}
	return pdCli, etcdCli, err
}

func CreatePDClient(cfg *config.Config) (*pdclient.APIClient, error) {
	if cfg == nil || len(cfg.PD.Endpoints) == 0 {
		return nil, errors.New("need specify pd endpoints")
	}
	var pdCli *pdclient.APIClient
	var err error
	for _, endpoint := range cfg.PD.Endpoints {
		pdCli, err = pdclient.NewAPIClient(httpclient.APIClientConfig{
			// TODO: support all PD endpoints.
			Endpoint: fmt.Sprintf("%v://%v", cfg.GetHTTPScheme(), endpoint),
			Context:  context.Background(),
			TLS:      cfg.Security.GetTLSConfig(),
		})
		if err == nil {
			_, err = pdCli.GetHealth()
			if err == nil {
				log.Info("create pd client success", zap.String("pd-address", endpoint))
				return pdCli, nil
			}
		}
	}
	if err != nil {
		return nil, err
	}
	if pdCli == nil {
		return nil, fmt.Errorf("can't create pd client, should never happen")
	}
	return pdCli, err
}
