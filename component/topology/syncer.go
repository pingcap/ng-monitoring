package topology

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/pingcap/ng-monitoring/component/domain"
	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/ng-monitoring/utils"
	"github.com/pingcap/ng-monitoring/utils/printer"

	"github.com/pingcap/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

const (
	topologyPrefix          = "/topology/ng-monitoring"
	dashboardAuthScheme     = "token-v1"
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
	do              *domain.Domain
	topologySession *concurrency.Session
	serverInfo      *ServerInfo
	ctx             context.Context
	cancel          context.CancelFunc
}

func NewTopologySyncer(do *domain.Domain) (*TopologySyncer, error) {
	dashboardAuthToken, err := newDashboardAuthToken()
	if err != nil {
		return nil, fmt.Errorf("generate dashboard authentication token: %w", err)
	}
	syncer := &TopologySyncer{
		do:         do,
		serverInfo: getServerInfo(dashboardAuthToken),
	}
	syncer.ctx, syncer.cancel = context.WithCancel(context.Background())
	return syncer, nil
}

func newDashboardAuthToken() (string, error) {
	data := make([]byte, 32)
	if _, err := rand.Read(data); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(data), nil
}

func (s *TopologySyncer) Start() {
	go utils.GoWithRecovery(s.topologyInfoKeeperLoop, nil)
}

func (s *TopologySyncer) topologyInfoKeeperLoop() {
	err := s.refreshTopologyInfo()
	if err != nil {
		log.Error("store topology into etcd failed", zap.Error(err))
	}
	ticker := time.NewTicker(topologyTimeToRefresh)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			err := s.refreshTopologyInfo()
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

func (s *TopologySyncer) refreshTopologyInfo() error {
	if s.topologySession == nil {
		return s.newTopologySessionAndStoreServerInfo()
	}
	return s.storeServerAndTopologyInfo()
}

func (s *TopologySyncer) newTopologySessionAndStoreServerInfo() error {
	etcdCli, err := s.do.GetEtcdClient()
	if err != nil {
		return err
	}
	session, err := newEtcdSession(s.ctx, etcdCli, defaultRetryCnt, topologySessionTTL)
	if err != nil {
		return err
	}
	s.topologySession = session

	return s.storeServerAndTopologyInfo()
}

func (s *TopologySyncer) storeServerAndTopologyInfo() error {
	if s.topologySession == nil {
		return fmt.Errorf("topology session is not initialized")
	}

	etcdCli, err := s.do.GetEtcdClient()
	if err != nil {
		return err
	}

	cfg := config.GetGlobalConfig()
	infoKey := fmt.Sprintf("%s/%s/info", topologyPrefix, cfg.AdvertiseAddress)
	infoBuf, err := json.Marshal(s.serverInfo)
	if err != nil {
		return err
	}
	ttlKey := fmt.Sprintf("%s/%s/ttl", topologyPrefix, cfg.AdvertiseAddress)
	return putTopologyToEtcd(
		s.ctx,
		etcdCli,
		defaultRetryCnt,
		infoKey,
		string(infoBuf),
		ttlKey,
		fmt.Sprintf("%v", time.Now().UnixNano()),
		s.topologySession.Lease(),
	)
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

func putTopologyToEtcd(
	ctx context.Context,
	etcdCli *clientv3.Client,
	retryCnt int,
	infoKey, infoValue, ttlKey, ttlValue string,
	leaseID clientv3.LeaseID,
) error {
	var err error
	for i := 0; i < retryCnt; i++ {
		if isContextDone(ctx) {
			return ctx.Err()
		}

		childCtx, cancel := context.WithTimeout(ctx, defaultTimeout)
		_, err = etcdCli.Txn(childCtx).Then(
			// Server information is persistent, while liveness is bound to the
			// current topology session. Publishing both in one transaction keeps
			// the advertised Dashboard credential consistent with liveness.
			clientv3.OpPut(infoKey, infoValue),
			clientv3.OpPut(ttlKey, ttlValue, clientv3.WithLease(leaseID)),
		).Commit()
		cancel()
		if err == nil {
			return nil
		}
		// Topology values may contain internal credentials. Never include them
		// in logs when an etcd write fails.
		log.Warn("[syncer] etcd-cli put topology failed",
			zap.Strings("keys", []string{infoKey, ttlKey}),
			zap.Error(err),
			zap.Int("retryCnt", i))
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

// ServerInfo is server static information.
// It will not be updated when server running.
// So please only put static information in ServerInfo struct.
type ServerInfo struct {
	GitHash        string             `json:"git_hash"`
	IP             string             `json:"ip"`
	Port           uint64             `json:"listening_port"`
	StartTimestamp int64              `json:"start_timestamp"`
	DashboardAuth  *DashboardAuthInfo `json:"dashboard_auth,omitempty"`
}

type DashboardAuthInfo struct {
	Scheme string `json:"scheme"`
	Token  string `json:"token"`
}

func getServerInfo(dashboardAuthToken string) *ServerInfo {
	info := &ServerInfo{
		GitHash:        printer.NGMGitHash,
		IP:             "",
		Port:           0,
		StartTimestamp: time.Now().Unix(),
		DashboardAuth: &DashboardAuthInfo{
			Scheme: dashboardAuthScheme,
			Token:  dashboardAuthToken,
		},
	}
	cfg := config.GetGlobalConfig()
	host, port, err := net.SplitHostPort(cfg.AdvertiseAddress)
	if err == nil {
		info.IP = host
		info.Port, _ = strconv.ParseUint(port, 10, 64)
	}
	return info
}
