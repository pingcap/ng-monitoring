package globalconfig

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/zhongzc/ng_monitoring/utils"
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

var loader *GlobalConfigLoader

type GlobalConfigLoader struct {
	cli    *clientv3.Client
	cfg    *GlobalConfig
	cancel context.CancelFunc
}

type GlobalConfig struct {
	EnableTopSQL bool
}

func Init(cli *clientv3.Client) {
	loader = &GlobalConfigLoader{
		cli: cli,
	}
	go utils.GoWithRecovery(loader.start, nil)
	return
}

func GetGlobalConfig() *GlobalConfig {
	return loader.cfg
}

func (g *GlobalConfigLoader) start() {
	ctx, cancel := context.WithCancel(context.Background())
	g.cancel = cancel
	g.LoadGlobalConfigLoop(ctx)
}

func Stop() {
	if loader != nil && loader.cancel != nil {
		loader.cancel()
	}
}

func (g *GlobalConfigLoader) LoadGlobalConfigLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	watchCh := g.cli.Watch(ctx, globalConfigPath, clientv3.WithPrefix())
	var err error
	g.cfg, err = g.loadAllGlobalConfig(ctx)
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
				if g.cfg == nil && *g.cfg != *cfg {
					g.cfg = cfg
					log.Info("load global config", zap.Reflect("cfg", g.cfg))
				}
			}
		case e, ok := <-watchCh:
			if !ok {
				log.Info("global config watch channel closed")
				watchCh = g.cli.Watch(ctx, globalConfigPath, clientv3.WithPrefix())
			} else {
				if g.cfg == nil {
					g.cfg = &GlobalConfig{}
				}
				for _, event := range e.Events {
					if event.Type != mvccpb.PUT {
						continue
					}
					err = g.parseGlobalConfig(string(event.Kv.Key), string(event.Kv.Value), g.cfg)
					if err != nil {
						log.Error("load global config failed", zap.Error(err))
					}
					log.Info("watch global config changed", zap.Reflect("cfg", g.cfg))
				}
			}
		}
	}
}

func (g *GlobalConfigLoader) loadAllGlobalConfig(ctx context.Context) (*GlobalConfig, error) {
	var err error
	var resp *clientv3.GetResponse
	for i := 0; i < defaultRetryCnt; i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		childCtx, cancel := context.WithTimeout(ctx, defaultTimeout)
		resp, err = g.cli.Get(childCtx, globalConfigPath, clientv3.WithPrefix())
		cancel()
		if err != nil {
			log.Debug("load global config failed.", zap.Error(err))
			time.Sleep(defaultRetryInterval)
			continue
		}
		if len(resp.Kvs) == 0 {
			return nil, nil
		}
		cfg := GlobalConfig{}
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

func (g *GlobalConfigLoader) parseGlobalConfig(key, value string, cfg *GlobalConfig) error {
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
