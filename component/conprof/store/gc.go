package store

import (
	"time"

	"github.com/pingcap/ng-monitoring/component/conprof/meta"
	"github.com/pingcap/ng-monitoring/config"

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
		err := s.db.ConprofDeleteProfileDataBeforeTs(s.ctx, info.ID, safePointTs)
		if err != nil {
			log.Error("gc delete target data failed", zap.Error(err))
		}
		err = s.db.ConprofDeleteProfileMetaBeforeTs(s.ctx, info.ID, safePointTs)
		if err != nil {
			log.Error("gc delete target meta failed", zap.Error(err))
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
	targets := make([]meta.ProfileTarget, 0, 16)
	infos := make([]meta.TargetInfo, 0, 16)
	err := s.db.ConprofQueryAllProfileTargets(s.ctx, func(target meta.ProfileTarget, info meta.TargetInfo) error {
		s.rebaseID(info.ID)
		targets = append(targets, target)
		infos = append(infos, info)
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	log.Info("gc load all target info from meta table", zap.Int("all-target-count", len(targets)))
	return targets, infos, nil
}

func (s *ProfileStorage) getLastSafePointTs() int64 {
	cfg := config.GetGlobalConfig()
	safePoint := time.Now().Add(time.Duration(-cfg.ContinueProfiling.DataRetentionSeconds) * time.Second)
	return safePoint.Unix()
}
