package pdvariable

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ng_monitoring/utils"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	globalConfigPath     = "/global/config/"
	defaultRetryCnt      = 5
	defaultTimeout       = time.Second
	defaultRetryInterval = time.Millisecond * 200
)

var loader *VariableLoader

type VariableLoader struct {
	getCli func() *clientv3.Client
	cfg    atomic.Value
	cancel context.CancelFunc
}

type PDVariable struct {
	EnableTopSQL bool
}

func Init(getCli func() *clientv3.Client) {
	loader = &VariableLoader{
		getCli: getCli,
	}
	go utils.GoWithRecovery(loader.start, nil)
	return
}

func GetPDVariable() *PDVariable {
	return loader.getPDVariable()
}

func (g *VariableLoader) start() {
	ctx, cancel := context.WithCancel(context.Background())
	g.cancel = cancel
	g.loadGlobalConfigLoop(ctx)
}

func Stop() {
	if loader != nil && loader.cancel != nil {
		loader.cancel()
	}
}

func (g *VariableLoader) loadGlobalConfigLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	watchCh := g.getCli().Watch(ctx, globalConfigPath, clientv3.WithPrefix())
	cfg, err := g.loadAllGlobalConfig(ctx)
	g.cfg.Store(cfg)
	if err != nil {
		log.Error("first load global config failed", zap.Error(err))
	} else {
		log.Info("first load global config", zap.Reflect("global-config", g.cfg))
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cfg, err := g.loadAllGlobalConfig(ctx)
			if err != nil {
				log.Error("load global config failed", zap.Error(err))
			} else if cfg != nil {
				oldCfg := g.getPDVariable()
				if oldCfg == nil || *oldCfg != *cfg {
					g.cfg.Store(cfg)
					log.Info("load global config", zap.Reflect("cfg", g.cfg))
				}
			}
		case e, ok := <-watchCh:
			if !ok {
				log.Info("global config watch channel closed")
				watchCh = g.getCli().Watch(ctx, globalConfigPath, clientv3.WithPrefix())
				// sleep a while to avoid too often.
				time.Sleep(time.Second)
			} else {
				cfg := *g.getPDVariable()
				for _, event := range e.Events {
					if event.Type != mvccpb.PUT {
						continue
					}
					err = g.parseGlobalConfig(string(event.Kv.Key), string(event.Kv.Value), &cfg)
					if err != nil {
						log.Error("load global config failed", zap.Error(err))
					}
					log.Info("watch global config changed", zap.Reflect("cfg", cfg))
				}
				g.cfg.Store(cfg)
			}
		}
	}
}

func (g *VariableLoader) loadAllGlobalConfig(ctx context.Context) (*PDVariable, error) {
	var err error
	var resp *clientv3.GetResponse
	for i := 0; i < defaultRetryCnt; i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		childCtx, cancel := context.WithTimeout(ctx, defaultTimeout)
		resp, err = g.getCli().Get(childCtx, globalConfigPath, clientv3.WithPrefix())
		cancel()
		if err != nil {
			log.Debug("load global config failed.", zap.Error(err))
			time.Sleep(defaultRetryInterval)
			continue
		}
		if len(resp.Kvs) == 0 {
			return nil, nil
		}
		cfg := PDVariable{}
		for _, kv := range resp.Kvs {
			err = g.parseGlobalConfig(string(kv.Key), string(kv.Value), &cfg)
			if err != nil {
				return nil, err
			}
		}
		return &cfg, nil
	}
	return nil, err
}

func (g *VariableLoader) parseGlobalConfig(key, value string, cfg *PDVariable) error {
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

func (g *VariableLoader) getPDVariable() *PDVariable {
	variable := g.cfg.Load()
	if variable == nil {
		return nil
	}
	return variable.(*PDVariable)
}
