package store

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/pingcap/ng-monitoring/component/conprof/meta"
	"github.com/pingcap/ng-monitoring/database/docdb"
	"github.com/pingcap/ng-monitoring/utils"

	"github.com/pingcap/log"
	"github.com/valyala/gozstd"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

var ErrStoreIsClosed = errors.New("storage is closed")

type ProfileStorage struct {
	closed atomic.Bool
	ctx    context.Context
	cancel context.CancelFunc
	sync.Mutex
	db          docdb.DocDB
	metaCache   map[meta.ProfileTarget]*meta.TargetInfo
	idAllocator int64
}

func NewProfileStorage(db docdb.DocDB) (*ProfileStorage, error) {
	store := &ProfileStorage{
		db:        db,
		metaCache: make(map[meta.ProfileTarget]*meta.TargetInfo),
	}
	store.ctx, store.cancel = context.WithCancel(context.Background())
	err := store.init()
	if err != nil {
		return nil, err
	}

	go utils.GoWithRecovery(store.doGCLoop, nil)

	return store, nil
}

func (s *ProfileStorage) init() error {
	allTargets, allInfos, err := s.loadAllTargetsFromTable()
	if err != nil {
		return err
	}
	for i, target := range allTargets {
		info := allInfos[i]
		s.metaCache[target] = &info
	}
	return nil
}

func (s *ProfileStorage) loadMetaIntoCache(target meta.ProfileTarget) error {
	return s.db.ConprofQueryTargetInfo(s.ctx, target, func(info meta.TargetInfo) error {
		s.rebaseID(info.ID)
		s.metaCache[target] = &info
		log.Info("load target info into cache",
			zap.String("component", target.Component),
			zap.String("address", target.Address),
			zap.String("kind", target.Kind),
			zap.Int64("id", info.ID),
			zap.Int64("ts", info.LastScrapeTs))
		return nil
	})
}

func (s *ProfileStorage) UpdateProfileTargetInfo(pt meta.ProfileTarget, ts int64) (bool, error) {
	if s.isClose() {
		return false, ErrStoreIsClosed
	}
	s.Lock()
	info := s.metaCache[pt]
	s.Unlock()
	if info == nil || ts <= info.LastScrapeTs {
		return false, nil
	}
	info.LastScrapeTs = ts
	err := s.db.ConprofUpdateTargetInfo(s.ctx, *info)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *ProfileStorage) AddProfile(pt meta.ProfileTarget, t time.Time, profileData []byte, profileErr error) error {
	if s.isClose() {
		return ErrStoreIsClosed
	}
	ts := t.Unix()
	info, err := s.prepareProfileTable(pt, ts)
	if err != nil {
		return err
	}

	var errStr string
	if profileErr == nil {
		if pt.Kind == meta.ProfileKindGoroutine {
			profileData = gozstd.Compress(nil, profileData)
		}
		err = s.db.ConprofWriteProfileData(s.ctx, info.ID, ts, profileData)
		if err != nil {
			return err
		}
	} else {
		errStr = profileErr.Error()
	}
	return s.db.ConprofWriteProfileMeta(s.ctx, info.ID, ts, errStr)
}

type QueryLimiter struct {
	cnt   *atomic.Int64
	limit int64
}

func newQueryLimiter(limit int64) *QueryLimiter {
	return &QueryLimiter{
		cnt:   atomic.NewInt64(0),
		limit: limit,
	}
}

func (l *QueryLimiter) Add(n int) {
	l.cnt.Add(int64(n))
}

func (l *QueryLimiter) IsFull() bool {
	return l.cnt.Load() >= l.limit
}

var (
	errResultFull         = errors.New("reach the query limit")
	errQueryRangeTooLarge = errors.New("query time range too large, should no more than 2 hours")
)

func (s *ProfileStorage) checkParam(param *meta.BasicQueryParam) error {
	if param.End-param.Begin > 2*60*60 {
		return errQueryRangeTooLarge
	}
	if param.Limit == 0 {
		param.Limit = math.MaxInt64
	}
	return nil
}

func (s *ProfileStorage) QueryGroupProfiles(param *meta.BasicQueryParam) ([]meta.ProfileList, error) {
	if s.isClose() {
		return nil, ErrStoreIsClosed
	}
	if param == nil {
		return nil, nil
	}
	err := s.checkParam(param)
	if err != nil {
		return nil, err
	}
	targets := param.Targets
	if len(targets) == 0 {
		targets = s.getAllTargetsFromCache()
	}

	type queryResult struct {
		list meta.ProfileList
		err  error
	}
	resultCh := make(chan queryResult, len(targets))
	var wg sync.WaitGroup
	rateLimiter := utils.NewRateLimit(16)
	doneCh := make(chan struct{})
	for _, pt := range targets {
		info := s.GetTargetInfoFromCache(pt)
		if info == nil {
			continue
		}
		target := pt
		wg.Add(1)
		go utils.GoWithRecovery(func() {
			rateLimiter.GetToken(doneCh)
			defer func() {
				rateLimiter.PutToken()
				wg.Done()
			}()
			list, err := s.QueryTargetProfiles(target, info, param)
			resultCh <- queryResult{list: list, err: err}
		}, nil)
	}
	wg.Wait()
	close(resultCh)

	result := make([]meta.ProfileList, 0, len(resultCh))
	for qr := range resultCh {
		if qr.err != nil {
			return nil, qr.err
		}
		if len(qr.list.TsList) == 0 {
			continue
		}
		result = append(result, qr.list)
	}
	return result, nil
}

func (s *ProfileStorage) QueryTargetProfiles(pt meta.ProfileTarget, ptInfo *meta.TargetInfo, param *meta.BasicQueryParam) (meta.ProfileList, error) {
	queryLimiter := newQueryLimiter(param.Limit)
	result := meta.ProfileList{Target: pt}
	err := s.db.ConprofQueryProfileMeta(s.ctx, ptInfo.ID, param.Begin, param.End, func(ts int64, verr string) error {
		result.TsList = append(result.TsList, ts)
		result.ErrorList = append(result.ErrorList, verr)
		queryLimiter.Add(1)
		if queryLimiter.IsFull() {
			return errResultFull
		}
		return nil
	})
	if err == errResultFull {
		return result, nil
	}
	return result, err
}

func (s *ProfileStorage) QueryProfileData(param *meta.BasicQueryParam, handleFn func(meta.ProfileTarget, int64, []byte) error) error {
	if s.isClose() {
		return ErrStoreIsClosed
	}
	if param == nil || handleFn == nil {
		return nil
	}
	err := s.checkParam(param)
	if err != nil {
		return err
	}
	targets := param.Targets
	if len(targets) == 0 {
		targets = s.getAllTargetsFromCache()
	}

	var fnMu sync.Mutex
	safeHandleFn := func(pt meta.ProfileTarget, ts int64, data []byte) error {
		fnMu.Lock()
		defer fnMu.Unlock()
		return handleFn(pt, ts, data)
	}

	errCh := make(chan error, len(targets))
	var wg sync.WaitGroup
	rateLimiter := utils.NewRateLimit(16)
	doneCh := make(chan struct{})
	for _, pt := range targets {
		info := s.GetTargetInfoFromCache(pt)
		if info == nil {
			continue
		}

		target := pt
		wg.Add(1)
		go utils.GoWithRecovery(func() {
			rateLimiter.GetToken(doneCh)
			defer func() {
				rateLimiter.PutToken()
				wg.Done()
			}()
			err := s.QueryTargetProfileData(target, info, param, safeHandleFn)
			errCh <- err
		}, nil)
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *ProfileStorage) QueryTargetProfileData(pt meta.ProfileTarget, ptInfo *meta.TargetInfo, param *meta.BasicQueryParam, handleFn func(meta.ProfileTarget, int64, []byte) error) error {
	queryLimiter := newQueryLimiter(param.Limit)
	err := s.db.ConprofQueryProfileData(s.ctx, ptInfo.ID, param.Begin, param.End, func(ts int64, data []byte) error {
		var err error
		if pt.Kind == meta.ProfileKindGoroutine {
			data, err = gozstd.Decompress(nil, data)
			if err != nil {
				return err
			}
		}
		err = handleFn(pt, ts, data)
		if err != nil {
			return err
		}
		queryLimiter.Add(1)
		if queryLimiter.IsFull() {
			return errResultFull
		}
		return nil
	})
	if err == errResultFull {
		return nil
	}
	return err
}

func (s *ProfileStorage) GetTargetInfoFromCache(pt meta.ProfileTarget) *meta.TargetInfo {
	s.Lock()
	info := s.metaCache[pt]
	s.Unlock()
	return info
}

func (s *ProfileStorage) getAllTargetsFromCache() []meta.ProfileTarget {
	s.Lock()
	defer s.Unlock()
	targets := make([]meta.ProfileTarget, 0, len(s.metaCache))
	for pt := range s.metaCache {
		targets = append(targets, pt)
	}
	return targets
}

func (s *ProfileStorage) Close() {
	if s.isClose() {
		return
	}
	s.closed.Store(true)
	if s.cancel != nil {
		s.cancel()
	}
}

func (s *ProfileStorage) isClose() bool {
	return s.closed.Load()
}

func (s *ProfileStorage) prepareProfileTable(pt meta.ProfileTarget, ts int64) (*meta.TargetInfo, error) {
	s.Lock()
	defer s.Unlock()
	info := s.metaCache[pt]
	if info != nil {
		return info, nil
	}
	err := s.loadMetaIntoCache(pt)
	if err != nil {
		return nil, err
	}
	info = s.metaCache[pt]
	if info != nil {
		return info, nil
	}
	info, err = s.createProfileTable(pt, ts)
	if err != nil {
		return info, err
	}
	// update cache
	s.metaCache[pt] = info
	return info, nil
}

func (s *ProfileStorage) createProfileTable(pt meta.ProfileTarget, ts int64) (*meta.TargetInfo, error) {
	info := &meta.TargetInfo{
		ID:           s.allocID(),
		LastScrapeTs: ts,
	}
	if err := s.db.ConprofCreateProfileTables(s.ctx, info.ID); err != nil {
		return nil, err
	}
	if err := s.db.ConprofCreateTargetInfo(s.ctx, pt, *info); err != nil {
		return nil, err
	}
	log.Info("create profile target table",
		zap.Int64("id", info.ID),
		zap.String("component", pt.Component),
		zap.String("address", pt.Address),
		zap.String("kind", pt.Kind))
	return info, nil
}

func (s *ProfileStorage) dropProfileTableIfStaled(pt meta.ProfileTarget, info meta.TargetInfo, safePointTs int64) error {
	s.Lock()
	defer s.Unlock()
	lastScrapeTs := info.LastScrapeTs
	cacheInfo := s.metaCache[pt]
	if cacheInfo != nil {
		if cacheInfo.ID != info.ID {
			log.Error("must be something wrong, same target has different id",
				zap.String("component", pt.Component),
				zap.String("address", pt.Address),
				zap.String("kind", pt.Kind),
				zap.Int64("id-1", cacheInfo.ID),
				zap.Int64("id-2", info.ID))
		} else {
			lastScrapeTs = cacheInfo.LastScrapeTs
		}
	}
	if lastScrapeTs >= safePointTs {
		return nil
	}

	if err := s.db.ConprofDeleteProfileTables(s.ctx, info.ID); err != nil {
		return err
	}
	delete(s.metaCache, pt)

	log.Info("drop profile target table",
		zap.Int64("id", info.ID),
		zap.String("component", pt.Component),
		zap.String("address", pt.Address),
		zap.String("kind", pt.Kind))
	return nil
}

func (s *ProfileStorage) rebaseID(id int64) {
	if id <= s.idAllocator {
		return
	}
	s.idAllocator = id
}

func (s *ProfileStorage) allocID() int64 {
	s.idAllocator += 1
	return s.idAllocator
}
