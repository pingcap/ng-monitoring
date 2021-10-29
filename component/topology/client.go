package topology

import (
	"context"
	"fmt"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb-dashboard/util/client/httpclient"
	"github.com/pingcap/tidb-dashboard/util/client/pdclient"
	"github.com/zhongzc/ng_monitoring/config"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

type Client struct {
	pdCli   *pdclient.APIClient
	etcdCli *clientv3.Client
	pdCfg   config.PD
}

func NewClient(cfg *config.Config) (*Client, error) {
	pdCli, etcdCli, err := createClient(cfg)
	if err != nil {
		return nil, err
	}
	return &Client{
		pdCli:   pdCli,
		etcdCli: etcdCli,
		pdCfg:   cfg.PD,
	}, nil
}

func (c *Client) reCreateClient(cfg *config.Config) {
	if c.pdCfg.Equal(cfg.PD) {
		return
	}
	err := c.etcdCli.Close()
	if err != nil {
		log.Error("close etcd client failed", zap.Error(err))
	}
	pdCli, etcdCli, err := createClient(cfg)
	if err != nil {
		log.Error("recreate pd/etcd client failed", zap.Error(err))
		return
	}
	c.pdCfg = cfg.PD
	c.pdCli = pdCli
	c.etcdCli = etcdCli
	log.Info("recreate pd/etcd client success")
}

func (c *Client) Close() error {
	return c.etcdCli.Close()
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

	var pdCli *pdclient.APIClient
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
				return pdCli, etcdCli, nil
			}
		}
	}
	if err != nil {
		return nil, nil, err
	}
	if pdCli == nil {
		return nil, nil, fmt.Errorf("can't create pd client, should never happen")
	}
	return pdCli, etcdCli, err
}
