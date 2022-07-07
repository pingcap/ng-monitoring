package domain

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/pingcap/ng-monitoring/config"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb-dashboard/util/client/httpclient"
	"github.com/pingcap/tidb-dashboard/util/client/pdclient"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	minRetryInterval = time.Millisecond * 10
	maxRetryInterval = time.Second
)

type ClientMaintainer struct {
	pdCli       atomic.Value // *pdclient.APIClient
	etcdCli     atomic.Value // *clientv3.Client
	pdCfg       config.PD
	initialized chan struct{}
}

func NewClientMaintainer() *ClientMaintainer {
	return &ClientMaintainer{
		initialized: make(chan struct{}),
	}
}

func (cm *ClientMaintainer) Init(pdCfg config.PD, pdCli *pdclient.APIClient, etcdCli *clientv3.Client) {
	cm.pdCfg = pdCfg
	cm.pdCli.Store(pdCli)
	cm.etcdCli.Store(etcdCli)
	close(cm.initialized)
}

// WARN: call this function will blocked until successfully created PD client.
func (cm *ClientMaintainer) GetPDClient(ctx context.Context) (*pdclient.APIClient, error) {
	err := cm.waitUntilInitialized(ctx)
	if err != nil {
		return nil, err
	}
	cli := cm.pdCli.Load()
	return cli.(*pdclient.APIClient), nil
}

// WARN: call this function will blocked until successfully created etcd client.
func (cm *ClientMaintainer) GetEtcdClient(ctx context.Context) (*clientv3.Client, error) {
	err := cm.waitUntilInitialized(ctx)
	if err != nil {
		return nil, err
	}
	cli := cm.etcdCli.Load()
	return cli.(*clientv3.Client), nil
}

func (cm *ClientMaintainer) waitUntilInitialized(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-cm.initialized:
		return nil
	}
}

func (cm *ClientMaintainer) IsInitialized() bool {
	select {
	case <-cm.initialized:
		return true
	default:
		return false
	}
}

func (cm *ClientMaintainer) NeedRecreateClient(pdCfg config.PD) bool {
	return !cm.pdCfg.Equal(pdCfg)
}

func (cm *ClientMaintainer) Close() {
	select {
	case <-cm.initialized:
		etcdCli := cm.etcdCli.Load()
		_ = etcdCli.(*clientv3.Client).Close()
	default:
		return
	}
}

func createClientWithRetry(ctx context.Context) (*pdclient.APIClient, *clientv3.Client, error) {
	latest := time.Now()
	backoff := minRetryInterval
	for {
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		default:
		}

		cfg := config.GetGlobalConfig()
		pdCli, etcdCli, err := createClient(&cfg)
		if err == nil {
			return pdCli, etcdCli, nil
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
		etcdCli.Close()
		return nil, nil, err
	}
	return pdCli, etcdCli, nil
}

func CreatePDClient(cfg *config.Config) (*pdclient.APIClient, error) {
	if cfg == nil || len(cfg.PD.Endpoints) == 0 {
		return nil, errors.New("need specify pd endpoints")
	}
	var pdCli *pdclient.APIClient
	var err error
	for _, endpoint := range cfg.PD.Endpoints {
		pdCli = pdclient.NewAPIClient(httpclient.Config{
			// TODO: support all PD endpoints.
			DefaultBaseURL: fmt.Sprintf("%v://%v", cfg.GetHTTPScheme(), endpoint),
			DefaultCtx:     context.Background(),
			TLSConfig:      cfg.Security.GetTLSConfig(),
		})
		_, err = pdCli.GetHealth(context.Background())
		if err == nil {
			log.Info("create pd client success", zap.String("pd-address", endpoint))
			return pdCli, nil
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
