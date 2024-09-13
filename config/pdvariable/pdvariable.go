package pdvariable

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/ng-monitoring/component/domain"
	"github.com/pingcap/ng-monitoring/utils"

	"github.com/pingcap/log"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

var (
	GlobalConfigPath     = "/global/config/"
	defaultRetryCnt      = 5
	defaultTimeout       = time.Second
	defaultRetryInterval = time.Millisecond * 200
)

func Init(do *domain.Domain) {
	loader = &variableLoader{do: do}
	defVar := DefaultPDVariable()
	loader.variable.Store(*defVar)
	go utils.GoWithRecovery(loader.start, nil)
}

type PDVariable struct {
	EnableTopSQL bool
}

func DefaultPDVariable() *PDVariable {
	return &PDVariable{EnableTopSQL: false}
}

type Subscriber = chan GetLatestPDVariable
type GetLatestPDVariable = func() PDVariable

func Subscribe() Subscriber {
	return loader.subscribe()
}

var loader *variableLoader

type variableLoader struct {
	do     *domain.Domain
	cancel context.CancelFunc

	variable atomic.Value

	sync.Mutex
	subscribers []Subscriber
}

func (l *variableLoader) start() {
	ctx, cancel := context.WithCancel(context.Background())
	l.cancel = cancel
	l.loadGlobalConfigLoop(ctx)
}

func Stop() {
	if loader != nil && loader.cancel != nil {
		loader.cancel()

		loader.Lock()
		for _, ch := range loader.subscribers {
			close(ch)
		}
		loader.Unlock()
	}
}

func (l *variableLoader) GetEtcdClient() (*clientv3.Client, error) {
	return l.do.GetEtcdClient()
}

func (l *variableLoader) loadGlobalConfigLoop(ctx context.Context) {
	etcdCli, err := l.do.GetEtcdClient()
	if err != nil {
		return
	}

	ticker := time.NewTicker(time.Minute)
	watchCh := etcdCli.Watch(ctx, GlobalConfigPath, clientv3.WithPrefix())

	cfg, err := l.loadAllGlobalConfig(ctx)
	if err != nil {
		log.Error("first load global config failed", zap.Error(err))
	} else {
		log.Info("first load global config", zap.Reflect("global-config", cfg))
		l.variable.Store(*cfg)
		l.notifySubscriber()
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			newCfg, err := l.loadAllGlobalConfig(ctx)
			if err != nil || newCfg == nil {
				log.Error("load global config failed", zap.Error(err))
			} else if cfg != nil {
				if newCfg != cfg {
					l.variable.Store(*newCfg)
					cfg = newCfg
					log.Info("load global config", zap.Reflect("cfg", cfg))
					l.notifySubscriber()
				}
			}
		case e, ok := <-watchCh:
			if !ok {
				log.Info("global config watch channel closed")
				etcdCli, err = l.do.GetEtcdClient()
				if err != nil {
					return
				}
				watchCh = etcdCli.Watch(ctx, GlobalConfigPath, clientv3.WithPrefix())
				// sleep a while to avoid too often.
				time.Sleep(time.Second)
			} else {
				newCfg := *cfg
				for _, event := range e.Events {
					if event.Type != mvccpb.PUT {
						continue
					}
					err = l.parseGlobalConfig(string(event.Kv.Key), string(event.Kv.Value), &newCfg)
					if err != nil {
						log.Error("load global config failed", zap.Error(err))
					}
					log.Info("watch global config changed", zap.Reflect("cfg", newCfg))
				}
				if newCfg != *cfg {
					l.variable.Store(newCfg)
					*cfg = newCfg
					l.notifySubscriber()
				}
			}
		}
	}
}

func (l *variableLoader) loadAllGlobalConfig(ctx context.Context) (*PDVariable, error) {
	var err error
	var resp *clientv3.GetResponse
	for i := 0; i < defaultRetryCnt; i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		var etcdCli *clientv3.Client
		etcdCli, err = l.do.GetEtcdClient()
		if err != nil {
			return nil, err
		}
		childCtx, cancel := context.WithTimeout(ctx, defaultTimeout)
		resp, err = etcdCli.Get(childCtx, GlobalConfigPath, clientv3.WithPrefix())
		cancel()
		if err != nil {
			log.Debug("load global config failed.", zap.Error(err))
			time.Sleep(defaultRetryInterval)
			continue
		}
		cfg := DefaultPDVariable()
		if len(resp.Kvs) == 0 {
			return cfg, nil
		}
		for _, kv := range resp.Kvs {
			err = l.parseGlobalConfig(string(kv.Key), string(kv.Value), cfg)
			if err != nil {
				return nil, err
			}
		}
		return cfg, nil
	}
	return nil, err
}

func (l *variableLoader) parseGlobalConfig(key, value string, cfg *PDVariable) error {
	key = strings.TrimPrefix(key, GlobalConfigPath)
	switch key {
	case "enable_resource_metering":
		v, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("global config %v has invalid value: %v",
				"enable_resource_metering", value)
		}
		cfg.EnableTopSQL = v
	}
	return nil
}

func (l *variableLoader) subscribe() Subscriber {
	ch := make(Subscriber, 1)
	l.Lock()
	l.subscribers = append(l.subscribers, ch)
	ch <- l.load
	l.Unlock()
	return ch
}

func (l *variableLoader) load() PDVariable {
	return l.variable.Load().(PDVariable)
}

func (l *variableLoader) notifySubscriber() {
	l.Lock()

	for _, ch := range l.subscribers {
		select {
		case ch <- l.load:
		default:
		}
	}

	l.Unlock()
}
