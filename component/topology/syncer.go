package topology

import (
	"context"
	"fmt"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ng_monitoring/config"
	"github.com/pingcap/ng_monitoring/utils"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.uber.org/zap"
)

const (
	topologyPrefix          = "/topology/ng-monitoring"
	defaultRetryCnt         = 3
	defaultTimeout          = 2 * time.Second
	defRetryInterval        = 30 * time.Millisecond
	newSessionRetryInterval = 200 * time.Millisecond
	logIntervalCnt          = int(3 * time.Second / newSessionRetryInterval)
	topologySessionTTL      = 45
)

var (
	topologyTimeToRefresh = 30 * time.Second
)

type TopologySyncer struct {
	topologySession *concurrency.Session
	ctx             context.Context
	cancel          context.CancelFunc
}

func NewTopologySyncer() *TopologySyncer {
	syncer := &TopologySyncer{}
	syncer.ctx, syncer.cancel = context.WithCancel(context.Background())
	return syncer
}

func (s *TopologySyncer) Start() {
	go utils.GoWithRecovery(s.topologyInfoKeeperLoop, nil)
}

func (s *TopologySyncer) topologyInfoKeeperLoop() {
	err := syncer.newTopologySessionAndStoreServerInfo()
	if err != nil {
		log.Error("store topology into etcd failed", zap.Error(err))
	}
	ticker := time.NewTicker(topologyTimeToRefresh)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			err := s.storeTopologyInfo()
			if err != nil {
				log.Error("refresh topology in loop failed", zap.Error(err))
			}
		case <-s.topologySessionDone():
			log.Info("server topology syncer need to restart")
			if err := s.newTopologySessionAndStoreServerInfo(); err != nil {
				log.Error("server topology syncer restart failed", zap.Error(err))
			} else {
				log.Info("server topology syncer restarted")
			}
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *TopologySyncer) newTopologySessionAndStoreServerInfo() error {
	etcdCli := GetEtcdClient()
	session, err := newEtcdSession(s.ctx, etcdCli, defaultRetryCnt, topologySessionTTL)
	if err != nil {
		return err
	}

	s.topologySession = session
	return s.storeTopologyInfo()
}

func (s *TopologySyncer) storeTopologyInfo() error {
	cfg := config.GetGlobalConfig()

	key := fmt.Sprintf("%s/%s/ttl", topologyPrefix, cfg.AdvertiseAddress)

	etcdCli := GetEtcdClient()
	return putKVToEtcd(s.ctx, etcdCli, defaultRetryCnt, key,
		fmt.Sprintf("%v", time.Now().UnixNano()),
		clientv3.WithLease(s.topologySession.Lease()))
}

func (s *TopologySyncer) topologySessionDone() <-chan struct{} {
	if s.topologySession == nil {
		return make(chan struct{}, 1)
	}
	return s.topologySession.Done()
}

func (s *TopologySyncer) Stop() {
	s.cancel()
}

func putKVToEtcd(ctx context.Context, etcdCli *clientv3.Client, retryCnt int, key, val string,
	opts ...clientv3.OpOption) error {
	var err error
	for i := 0; i < retryCnt; i++ {
		if isContextDone(ctx) {
			return ctx.Err()
		}

		childCtx, cancel := context.WithTimeout(ctx, defaultTimeout)
		_, err = etcdCli.Put(childCtx, key, val, opts...)
		cancel()
		if err == nil {
			return nil
		}
		log.Warn("[syncer] etcd-cli put kv failed", zap.String("key", key), zap.String("value", val), zap.Error(err), zap.Int("retryCnt", i))
		time.Sleep(defRetryInterval)
	}
	return err
}

func newEtcdSession(ctx context.Context, etcdCli *clientv3.Client, retryCnt, ttl int) (*concurrency.Session, error) {
	var err error
	var etcdSession *concurrency.Session
	failedCnt := 0
	for i := 0; i < retryCnt; i++ {
		if isContextDone(ctx) {
			return etcdSession, ctx.Err()
		}

		etcdSession, err = concurrency.NewSession(etcdCli,
			concurrency.WithTTL(ttl), concurrency.WithContext(ctx))
		if err == nil {
			break
		}
		if failedCnt%logIntervalCnt == 0 {
			log.Warn("failed to new session to etcd", zap.Error(err))
		}

		time.Sleep(newSessionRetryInterval)
		failedCnt++
	}
	return etcdSession, err
}

func isContextDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
	}
	return false
}
