package pdvariable

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ng_monitoring/utils"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.uber.org/zap"
)

const (
	globalConfigPath     = "/global/config/"
	defaultRetryCnt      = 5
	defaultTimeout       = time.Second
	defaultRetryInterval = time.Millisecond * 200
)

func Init(getCli func() *clientv3.Client) {
	loader = &variableLoader{
		getCli: getCli,
	}
	go utils.GoWithRecovery(loader.start, nil)
	return
}

type PDVariable struct {
	EnableTopSQL bool
}

func DefaultPDVariable() *PDVariable {
	return &PDVariable{EnableTopSQL: false}
}

type Subscriber = chan *PDVariable

func Subscribe() Subscriber {
	return loader.subscribe()
}

var loader *variableLoader

type variableLoader struct {
	getCli func() *clientv3.Client
	cfg    *PDVariable
	cancel context.CancelFunc

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
	}
}

func (l *variableLoader) loadGlobalConfigLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	watchCh := l.getCli().Watch(ctx, globalConfigPath, clientv3.WithPrefix())

	var err error
	l.cfg, err = l.loadAllGlobalConfig(ctx)
	if err != nil {
		log.Error("first load global config failed", zap.Error(err))
	} else {
		log.Info("first load global config", zap.Reflect("global-config", l.cfg))
		l.notifySubscriber()
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cfg, err := l.loadAllGlobalConfig(ctx)
			if err != nil {
				log.Error("load global config failed", zap.Error(err))
			} else if cfg != nil {
				if l.cfg == nil || *l.cfg != *cfg {
					l.cfg = cfg
					log.Info("load global config", zap.Reflect("cfg", l.cfg))
					l.notifySubscriber()
				}
			}
		case e, ok := <-watchCh:
			if !ok {
				log.Info("global config watch channel closed")
				watchCh = l.getCli().Watch(ctx, globalConfigPath, clientv3.WithPrefix())
				// sleep a while to avoid too often.
				time.Sleep(time.Second)
			} else {
				if l.cfg == nil {
					l.cfg = DefaultPDVariable()
				}
				for _, event := range e.Events {
					if event.Type != mvccpb.PUT {
						continue
					}
					err = l.parseGlobalConfig(string(event.Kv.Key), string(event.Kv.Value), l.cfg)
					if err != nil {
						log.Error("load global config failed", zap.Error(err))
					}
					log.Info("watch global config changed", zap.Reflect("cfg", l.cfg))
				}
				l.notifySubscriber()
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
		childCtx, cancel := context.WithTimeout(ctx, defaultTimeout)
		resp, err = l.getCli().Get(childCtx, globalConfigPath, clientv3.WithPrefix())
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
	if strings.HasPrefix(key, globalConfigPath) {
		key = key[len(globalConfigPath):]
	}
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
	l.Unlock()
	return ch
}

func (l *variableLoader) notifySubscriber() {
	l.Lock()

	for _, ch := range l.subscribers {
		select {
		case ch <- l.cfg:
		default:
		}
	}

	l.Unlock()
}
