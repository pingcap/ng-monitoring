package domain

import (
	"context"

	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/ng-monitoring/utils"

	"github.com/pingcap/tidb-dashboard/util/client/pdclient"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Domain struct {
	ctx         context.Context
	cancel      context.CancelFunc
	cm          *ClientMaintainer
	cfgChangeCh config.Subscriber
}

func NewDomain() *Domain {
	cfgSub := config.Subscribe()
	getCurCfg := <-cfgSub
	curCfg := getCurCfg()

	ctx, cancel := context.WithCancel(context.Background())
	do := &Domain{
		ctx:         ctx,
		cancel:      cancel,
		cm:          NewClientMaintainer(),
		cfgChangeCh: cfgSub,
	}
	go utils.GoWithRecovery(func() {
		do.start(curCfg)
	}, nil)
	return do
}

func NewDomainForTest(pdCli *pdclient.APIClient, etcdCli *clientv3.Client) *Domain {
	cfgSub := config.Subscribe()
	getCurCfg := <-cfgSub
	curCfg := getCurCfg()

	ctx, cancel := context.WithCancel(context.Background())
	do := &Domain{
		ctx:         ctx,
		cancel:      cancel,
		cm:          NewClientMaintainer(),
		cfgChangeCh: cfgSub,
	}
	do.cm.Init(curCfg.PD, pdCli, etcdCli)
	return do
}

// WARN: call this function will blocked until successfully created PD client.
func (do *Domain) GetPDClient() (*pdclient.APIClient, error) {
	return do.cm.GetPDClient(do.ctx)
}

// WARN: call this function will blocked until successfully created etcd client.
func (do *Domain) GetEtcdClient() (*clientv3.Client, error) {
	return do.cm.GetEtcdClient(do.ctx)
}

func (do *Domain) start(cfg config.Config) {
	err := do.createClientWithRetry(cfg)
	if err != nil {
		return
	}
	for {
		select {
		case <-do.ctx.Done():
			return
		case getCfg := <-do.cfgChangeCh:
			cfg = getCfg()
			err = do.createClientWithRetry(cfg)
			if err == context.Canceled {
				return
			}
		}
	}
}

func (do *Domain) createClientWithRetry(cfg config.Config) error {
	if do.cm.IsInitialized() {
		if !do.cm.NeedRecreateClient(cfg.PD) {
			return nil
		}
		do.cm.Close()
		do.cm = NewClientMaintainer()
	}
	pdCli, etcdCli, err := createClientWithRetry(do.ctx)
	if err != nil {
		return err
	}
	do.cm.Init(cfg.PD, pdCli, etcdCli)
	return nil
}

func (do *Domain) Close() {
	do.cm.Close()
	if do.cancel != nil {
		do.cancel()
	}
}
