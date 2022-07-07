package store

import (
	"fmt"
	"time"

	"github.com/pingcap/ng-monitoring/component/conprof/meta"
	"github.com/pingcap/ng-monitoring/config"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/types"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	gcInterval = time.Minute * 10
)

func (s *ProfileStorage) doGCLoop() {
	ticker := time.NewTicker(gcInterval)
	defer ticker.Stop()
	// run gc when started.
	s.runGC()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.runGC()
		}
	}
}

func (s *ProfileStorage) runGC() {
	start := time.Now()
	allTargets, allInfos, err := s.loadAllTargetsFromTable()
	if err != nil {
		log.Info("gc load all target info from meta table failed", zap.Error(err))
		return
	}
	safePointTs := s.getLastSafePointTs()
	for i, target := range allTargets {
		info := allInfos[i]
		sql := fmt.Sprintf("DELETE FROM %v WHERE ts <= ?", s.getProfileDataTableName(&info))
		err := s.db.Exec(sql, safePointTs)
		if err != nil {
			log.Error("gc delete target data failed", zap.Error(err))
		}
		sql = fmt.Sprintf("DELETE FROM %v WHERE ts <= ?", s.getProfileMetaTableName(&info))
		err = s.db.Exec(sql, safePointTs)
		if err != nil {
			log.Error("gc delete target data failed", zap.Error(err))
		}
		err = s.dropProfileTableIfStaled(target, info, safePointTs)
		if err != nil {
			log.Error("gc drop target table failed", zap.Error(err))
		}
	}
	log.Info("gc finished",
		zap.Int("total-targets", len(allTargets)),
		zap.Int64("safepoint", safePointTs),
		zap.Duration("cost", time.Since(start)))
}

func (s *ProfileStorage) loadAllTargetsFromTable() ([]meta.ProfileTarget, []meta.TargetInfo, error) {
	query := fmt.Sprintf("SELECT id, kind, component, address, last_scrape_ts FROM %v", metaTableName)
	res, err := s.db.Query(query)
	if err != nil {
		return nil, nil, err
	}
	defer res.Close()

	targets := make([]meta.ProfileTarget, 0, 16)
	infos := make([]meta.TargetInfo, 0, 16)
	err = res.Iterate(func(d types.Document) error {
		var id, ts int64
		var kind, component, address string
		err = document.Scan(d, &id, &kind, &component, &address, &ts)
		if err != nil {
			return err
		}
		s.rebaseID(id)
		target := meta.ProfileTarget{
			Kind:      kind,
			Component: component,
			Address:   address,
		}
		info := meta.TargetInfo{
			ID:           id,
			LastScrapeTs: ts,
		}
		targets = append(targets, target)
		infos = append(infos, info)
		return nil
	})
	log.Info("gc load all target info from meta table",
		zap.Int("all-target-count", len(targets)))
	return targets, infos, nil
}

func (s *ProfileStorage) getLastSafePointTs() int64 {
	cfg := config.GetGlobalConfig()
	safePoint := time.Now().Add(time.Duration(-cfg.ContinueProfiling.DataRetentionSeconds) * time.Second)
	return safePoint.Unix()
}
