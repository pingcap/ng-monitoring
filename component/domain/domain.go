package domain

import (
	"context"

	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/ng-monitoring/utils"
	"github.com/pingcap/tidb-dashboard/util/client/pdclient"
	"go.etcd.io/etcd/clientv3"
)

type Domain struct {
	ctx         context.Context
	cancel      context.CancelFunc
	cm          *ClientMaintainer
	cfgChangeCh config.Subscriber
}

func NewDomain() *Domain {
	do := &Domain{
		cm:          NewClientMaintainer(),
		cfgChangeCh: config.Subscribe(),
	}
	do.ctx, do.cancel = context.WithCancel(context.Background())
	go utils.GoWithRecovery(do.start, nil)
	return do
}

func NewDomainForTest(pdCli *pdclient.APIClient, etcdCli *clientv3.Client) *Domain {
	do := &Domain{
		cm:          NewClientMaintainer(),
		cfgChangeCh: config.Subscribe(),
	}
	do.ctx, do.cancel = context.WithCancel(context.Background())
	do.cm.Init(config.GetGlobalConfig().PD, pdCli, etcdCli)
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

func (do *Domain) start() {
	err := do.createClientWithRetry()
	if err != nil {
		return
	}
	for {
		select {
		case <-do.ctx.Done():
			return
		case <-do.cfgChangeCh:
			err = do.createClientWithRetry()
			if err == context.Canceled {
				return
			}
		}
	}
}

func (do *Domain) createClientWithRetry() error {
	if do.cm.IsInitialized() {
		if !do.cm.NeedRecreateClient(config.GetGlobalConfig().PD) {
			return nil
		}
		do.cm.Close()
		do.cm = NewClientMaintainer()
	}
	pdCli, etcdCli, err := createClientWithRetry(do.ctx)
	if err != nil {
		return err
	}
	do.cm.Init(config.GetGlobalConfig().PD, pdCli, etcdCli)
	return nil
}

func (do *Domain) Close() {
	do.cm.Close()
	if do.cancel != nil {
		do.cancel()
	}
}
