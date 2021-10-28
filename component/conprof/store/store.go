package store

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/types"
	"github.com/google/pprof/profile"
	"github.com/pingcap/log"
	"github.com/valyala/gozstd"
	"github.com/zhongzc/ng_monitoring/component/conprof/meta"
	"github.com/zhongzc/ng_monitoring/component/conprof/util"
	"github.com/zhongzc/ng_monitoring/utils"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	tableNamePrefix = "conprof"
	metaTableSuffix = "meta"
	dataTableSuffix = "data"
	metaTableName   = tableNamePrefix + "_targets_meta"
)

var ErrStoreIsClosed = errors.New("storage is closed")

type ProfileStorage struct {
	closed atomic.Bool
	sync.Mutex
	db           *genji.DB
	metaCache    map[meta.ProfileTarget]*meta.TargetInfo
	idAllocator  int64
	aliveTargets []meta.ProfileTarget
}

func NewProfileStorage(db *genji.DB) (*ProfileStorage, error) {
	store := &ProfileStorage{
		db:        db,
		metaCache: make(map[meta.ProfileTarget]*meta.TargetInfo),
	}
	err := store.init()
	if err != nil {
		return nil, err
	}

	go utils.GoWithRecovery(store.doGCLoop, nil)

	return store, nil
}

func (s *ProfileStorage) init() error {
	err := s.initMetaTable()
	if err != nil {
		return err
	}
	allTargets, allInfos, err := s.loadAllTargetsFromTable()
	for i, target := range allTargets {
		info := allInfos[i]
		s.metaCache[target] = &info
	}
	return nil
}

func (s *ProfileStorage) initMetaTable() error {
	// create meta table if not exists.
	sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %v (id INTEGER primary key, kind TEXT, component TEXT, address TEXT, last_scrape_ts INTEGER)", metaTableName)
	return s.db.Exec(sql)
}

func (s *ProfileStorage) loadMetaIntoCache(target meta.ProfileTarget) error {
	query := fmt.Sprintf("SELECT id, last_scrape_ts FROM %v WHERE kind = ? AND component = ? AND address = ?", metaTableName)
	res, err := s.db.Query(query, target.Kind, target.Component, target.Address)
	if err != nil {
		return err
	}
	defer res.Close()

	err = res.Iterate(func(d types.Document) error {
		var id, ts int64
		err = document.Scan(d, &id, &ts)
		if err != nil {
			return err
		}
		s.rebaseID(id)
		s.metaCache[target] = &meta.TargetInfo{
			ID:           id,
			LastScrapeTs: ts,
		}
		log.Info("load target info into cache",
			zap.String("component", target.Component),
			zap.String("address", target.Address),
			zap.String("kind", target.Kind),
			zap.Int64("id", id),
			zap.Int64("ts", ts))
		return nil
	})
	return err
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
	sql := fmt.Sprintf("UPDATE %v set last_scrape_ts = ? where id = ?", metaTableName)
	err := s.db.Exec(sql, ts, info.ID)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *ProfileStorage) AddProfile(pt meta.ProfileTarget, ts int64, profileData []byte) error {
	if s.isClose() {
		return ErrStoreIsClosed
	}
	info, err := s.prepareProfileTable(pt)
	if err != nil {
		return err
	}

	if pt.Kind == meta.ProfileKindGoroutine {
		profileData = gozstd.Compress(nil, profileData)
	} else {
		// todo: remove this.
		p, err := profile.ParseData(profileData)
		if err != nil {
			return err
		}
		bs := bytes.NewBuffer(nil)
		err = p.Write(bs)
		if err != nil {
			return err
		}
		profileData = bs.Bytes()
	}

	sql := fmt.Sprintf("INSERT INTO %v (ts, data) VALUES (?, ?)", s.getProfileDataTableName(info))
	err = s.db.Exec(sql, ts, profileData)
	if err != nil {
		return err
	}
	sql = fmt.Sprintf("INSERT INTO %v (ts) VALUES (?)", s.getProfileMetaTableName(info))
	err = s.db.Exec(sql, ts)
	if err != nil {
		return err
	}
	return nil
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

var errResultFull = errors.New("reach the query limit")

func (s *ProfileStorage) QueryGroupProfiles(param *meta.BasicQueryParam) ([]meta.ProfileList, error) {
	if s.isClose() {
		return nil, ErrStoreIsClosed
	}
	if param == nil {
		return nil, nil
	}
	if param.Limit == 0 {
		param.Limit = 1000
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
		info := s.getTargetInfoFromCache(pt)
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

	var result []meta.ProfileList
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
	args := []interface{}{param.Begin, param.End}
	query := fmt.Sprintf("SELECT ts FROM %v WHERE ts >= ? and ts <= ?", s.getProfileMetaTableName(ptInfo))
	res, err := s.db.Query(query, args...)
	if err != nil {
		return result, err
	}
	defer res.Close()
	err = res.Iterate(func(d types.Document) error {
		var ts int64
		err = document.Scan(d, &ts)
		if err != nil {
			return err
		}
		result.TsList = append(result.TsList, ts)
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
	if param.Limit == 0 {
		param.Limit = 1000
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
		info := s.getTargetInfoFromCache(pt)
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
	args := []interface{}{param.Begin, param.End}
	query := fmt.Sprintf("SELECT ts, data FROM %v WHERE ts >= ? and ts <= ?", s.getProfileDataTableName(ptInfo))
	res, err := s.db.Query(query, args...)
	if err != nil {
		return err
	}
	defer res.Close()
	err = res.Iterate(func(d types.Document) error {
		var ts int64
		var data []byte
		err = document.Scan(d, &ts, &data)
		if err != nil {
			return err
		}

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

func (s *ProfileStorage) getTargetInfoFromCache(pt meta.ProfileTarget) *meta.TargetInfo {
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
}

func (s *ProfileStorage) isClose() bool {
	return s.closed.Load()
}

func (s *ProfileStorage) prepareProfileTable(pt meta.ProfileTarget) (*meta.TargetInfo, error) {
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
	info, err = s.createProfileTable(pt)
	if err != nil {
		return info, err
	}
	// update cache
	s.metaCache[pt] = info
	return info, nil
}

func (s *ProfileStorage) createProfileTable(pt meta.ProfileTarget) (*meta.TargetInfo, error) {
	info := &meta.TargetInfo{
		ID:           s.allocID(),
		LastScrapeTs: util.GetTimeStamp(time.Now()),
	}
	tbName := s.getProfileMetaTableName(info)
	sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %v (ts INTEGER PRIMARY KEY)", tbName)
	err := s.db.Exec(sql)
	if err != nil {
		return info, err
	}
	tbName = s.getProfileDataTableName(info)
	sql = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %v (ts INTEGER PRIMARY KEY, data BLOB)", tbName)
	err = s.db.Exec(sql)
	if err != nil {
		return info, err
	}
	sql = fmt.Sprintf("INSERT INTO %v (id, kind, component, address, last_scrape_ts) VALUES (?, ?, ?, ?, ?)", metaTableName)
	err = s.db.Exec(sql, info.ID, pt.Kind, pt.Component, pt.Address, info.LastScrapeTs)
	if err != nil {
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
	// remove in meta table.
	sql := fmt.Sprintf("DELETE FROM %v WHERE id = ?", metaTableName)
	err := s.db.Exec(sql, info.ID)
	if err != nil {
		return err
	}

	// update cache
	delete(s.metaCache, pt)

	// drop the table
	sql = fmt.Sprintf("DROP TABLE IF EXISTS %v", s.getProfileDataTableName(&info))
	err = s.db.Exec(sql)
	if err != nil {
		return err
	}
	sql = fmt.Sprintf("DROP TABLE IF EXISTS %v", s.getProfileMetaTableName(&info))
	err = s.db.Exec(sql)
	if err != nil {
		return err
	}
	log.Info("drop profile target table",
		zap.Int64("id", info.ID),
		zap.String("component", pt.Component),
		zap.String("address", pt.Address),
		zap.String("kind", pt.Kind))
	return nil
}

func (s *ProfileStorage) getProfileMetaTableName(info *meta.TargetInfo) string {
	return fmt.Sprintf("`%v_%v_%v`", tableNamePrefix, info.ID, metaTableSuffix)
}

func (s *ProfileStorage) getProfileDataTableName(info *meta.TargetInfo) string {
	return fmt.Sprintf("`%v_%v_%v`", tableNamePrefix, info.ID, dataTableSuffix)
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
