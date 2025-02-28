package docdb

import (
	"context"
	"encoding/hex"
	"fmt"
	"path"
	"runtime"
	"strconv"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/engine/badgerengine"
	"github.com/genjidb/genji/errors"
	"github.com/genjidb/genji/types"
	"github.com/pingcap/log"
	"github.com/pingcap/ng-monitoring/component/conprof/meta"
	"github.com/pingcap/ng-monitoring/utils"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

type GenjiConfig struct {
	Path         string
	LogPath      string
	LogLevel     string
	BadgerConfig BadgerConfig
}

type BadgerConfig struct {
	LSMOnly                 bool    `toml:"lsm-only" json:"lsm_only"`
	SyncWrites              bool    `toml:"sync-writes" json:"sync_writes"`
	NumVersionsToKeep       int     `toml:"num-versions-to-keep" json:"num_versions_to_keep"`
	NumGoroutines           int     `toml:"num-goroutines" json:"num_goroutines"`
	MemTableSize            int64   `toml:"mem-table-size" json:"mem_table_size"`
	BaseTableSize           int64   `toml:"base-table-size" json:"base_table_size"`
	BaseLevelSize           int64   `toml:"base-level-size" json:"base_level_size"`
	LevelSizeMultiplier     int     `toml:"level-size-multiplier" json:"level_size_multiplier"`
	MaxLevels               int     `toml:"max-levels" json:"max_levels"`
	VLogPercentile          float64 `toml:"vlog-percentile" json:"vlog_percentile"`
	ValueThreshold          int64   `toml:"value-threshold" json:"value_threshold"`
	NumMemtables            int     `toml:"num-memtables" json:"num_memtables"`
	BlockSize               int     `toml:"block-size" json:"block_size"`
	BloomFalsePositive      float64 `toml:"bloom-false-positive" json:"bloom_false_positive"`
	BlockCacheSize          int64   `toml:"block-cache-size" json:"block_cache_size"`
	IndexCacheSize          int64   `toml:"index-cache-size" json:"index_cache_size"`
	NumLevelZeroTables      int     `toml:"num-level-zero-tables" json:"num_level_zero_tables"`
	NumLevelZeroTablesStall int     `toml:"num-level-zero-tables-stall" json:"num_level_zero_tables_stall"`
	ValueLogFileSize        int64   `toml:"value-log-file-size" json:"value_log_file_size"`
	ValueLogMaxEntries      uint32  `toml:"value-log-max-entries" json:"value_log_max_entries"`
	NumCompactors           int     `toml:"num-compactors" json:"num_compactors"`
	ZSTDCompressionLevel    int     `toml:"zstd-compression-level" json:"zstd_compression_level"`
}

type genjiDB struct {
	db      *genji.DB
	closeCh chan struct{}
}

func NewGenjiDB(ctx context.Context, cfg *GenjiConfig) (DocDB, error) {
	badger.DefaultIteratorOptions.PrefetchValues = false
	dataPath := path.Join(cfg.Path, "docdb")
	l, _ := initLogger(cfg.LogPath, cfg.LogLevel)
	var opts badger.Options
	if cfg.BadgerConfig.LSMOnly {
		opts = badger.LSMOnlyOptions(dataPath)
	} else {
		opts = badger.DefaultOptions(dataPath)
	}
	if !cfg.BadgerConfig.LSMOnly {
		opts = opts.WithValueThreshold(cfg.BadgerConfig.ValueThreshold)
	}
	opts = opts.
		WithLogger(l).
		WithSyncWrites(cfg.BadgerConfig.SyncWrites).
		WithNumVersionsToKeep(cfg.BadgerConfig.NumVersionsToKeep).
		WithNumGoroutines(cfg.BadgerConfig.NumGoroutines).
		WithMemTableSize(cfg.BadgerConfig.MemTableSize).
		WithBaseTableSize(cfg.BadgerConfig.BaseTableSize).
		WithBaseLevelSize(cfg.BadgerConfig.BaseLevelSize).
		WithLevelSizeMultiplier(cfg.BadgerConfig.LevelSizeMultiplier).
		WithMaxLevels(cfg.BadgerConfig.MaxLevels).
		WithVLogPercentile(cfg.BadgerConfig.VLogPercentile).
		WithNumMemtables(cfg.BadgerConfig.NumMemtables).
		WithBlockSize(cfg.BadgerConfig.BlockSize).
		WithBloomFalsePositive(cfg.BadgerConfig.BloomFalsePositive).
		WithBlockCacheSize(cfg.BadgerConfig.BlockCacheSize).
		WithIndexCacheSize(cfg.BadgerConfig.IndexCacheSize).
		WithNumLevelZeroTables(cfg.BadgerConfig.NumLevelZeroTables).
		WithNumLevelZeroTablesStall(cfg.BadgerConfig.NumLevelZeroTablesStall).
		WithValueLogFileSize(cfg.BadgerConfig.ValueLogFileSize).
		WithValueLogMaxEntries(cfg.BadgerConfig.ValueLogMaxEntries).
		WithNumCompactors(cfg.BadgerConfig.NumCompactors).
		WithZSTDCompressionLevel(cfg.BadgerConfig.ZSTDCompressionLevel)
	engine, err := badgerengine.NewEngine(opts)
	if err != nil {
		return nil, err
	}
	d := &genjiDB{closeCh: make(chan struct{})}
	go utils.GoWithRecovery(func() {
		doGCLoop(engine.DB, d.closeCh)
	}, nil)
	d.db, err = genji.New(ctx, engine)
	if err != nil {
		return nil, err
	}
	if err := d.tryInitTables(); err != nil {
		return nil, err
	}
	return d, nil
}

func NewGenjiDBFromGenji(g *genji.DB) (DocDB, error) {
	d := &genjiDB{db: g, closeCh: make(chan struct{})}
	if err := d.tryInitTables(); err != nil {
		return nil, err
	}
	return d, nil
}

func (d *genjiDB) tryInitTables() error {
	stmts := []string{
		"CREATE TABLE IF NOT EXISTS ng_monitoring_config (module TEXT primary key, config TEXT)",
		"CREATE TABLE IF NOT EXISTS sql_digest (digest VARCHAR(255) PRIMARY KEY)",
		"CREATE TABLE IF NOT EXISTS plan_digest (digest VARCHAR(255) PRIMARY KEY)",
		"CREATE TABLE IF NOT EXISTS conprof_targets_meta (id INTEGER primary key, kind TEXT, component TEXT, address TEXT, last_scrape_ts INTEGER)",
	}
	for _, stmt := range stmts {
		if err := d.db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func (d *genjiDB) Close() error {
	close(d.closeCh)
	return d.db.Close()
}

func (d *genjiDB) SaveConfig(ctx context.Context, cfg map[string]string) error {
	err := d.db.WithContext(ctx).Exec("DELETE FROM ng_monitoring_config")
	if err != nil {
		return err
	}
	for module, data := range cfg {
		err = d.db.WithContext(ctx).Exec("INSERT INTO ng_monitoring_config (module, config) VALUES (?, ?)", module, data)
		if err != nil {
			return err
		}
		log.Info("save config into storage", zap.String("module", module), zap.String("config", data))
	}
	return nil
}

func (d *genjiDB) LoadConfig(ctx context.Context) (map[string]string, error) {
	res, err := d.db.WithContext(ctx).Query("SELECT module, config FROM ng_monitoring_config")
	if err != nil {
		return nil, err
	}
	defer res.Close()
	cfgMap := make(map[string]string)
	err = res.Iterate(func(d types.Document) error {
		var module, cfg string
		err = document.Scan(d, &module, &cfg)
		if err != nil {
			return err
		}
		cfgMap[module] = cfg
		return nil
	})
	if err != nil {
		return nil, err
	}
	return cfgMap, nil
}

func (d *genjiDB) WriteSQLMeta(ctx context.Context, meta *tipb.SQLMeta) error {
	stmt := "INSERT INTO sql_digest(digest, sql_text, is_internal) VALUES (?, ?, ?) ON CONFLICT DO REPLACE"
	prepare, err := d.db.WithContext(ctx).Prepare(stmt)
	if err != nil {
		return err
	}
	return prepare.Exec(hex.EncodeToString(meta.SqlDigest), meta.NormalizedSql, meta.IsInternalSql)
}

func (d *genjiDB) QuerySQLMeta(ctx context.Context, digest string) (string, error) {
	r, err := d.db.WithContext(ctx).QueryDocument("SELECT sql_text FROM sql_digest WHERE digest = ?", digest)
	if err != nil {
		if err == errors.ErrDocumentNotFound {
			return "", nil
		}
		return "", err
	}
	var sql string
	if err := document.Scan(r, &sql); err != nil {
		return "", err
	}
	return sql, nil
}

func (d *genjiDB) DeleteSQLMetaBeforeTs(ctx context.Context, ts int64) error {
	return nil
}

func (d *genjiDB) QueryPlanMeta(ctx context.Context, digest string) (string, string, error) {
	r, err := d.db.WithContext(ctx).QueryDocument("SELECT plan_text, encoded_plan FROM plan_digest WHERE digest = ?", digest)
	if err != nil {
		if err == errors.ErrDocumentNotFound {
			return "", "", nil
		}
		return "", "", err
	}
	var text, encodedPlan string
	if err := document.Scan(r, &text, &encodedPlan); err != nil {
		return "", "", err
	}
	return text, encodedPlan, nil
}

func (d *genjiDB) WritePlanMeta(ctx context.Context, meta *tipb.PlanMeta) error {
	stmt := "INSERT INTO plan_digest(digest, plan_text, encoded_plan) VALUES (?, ?, ?) ON CONFLICT DO REPLACE"
	prepare, err := d.db.WithContext(ctx).Prepare(stmt)
	if err != nil {
		return err
	}
	return prepare.Exec(hex.EncodeToString(meta.PlanDigest), meta.NormalizedPlan, meta.EncodedNormalizedPlan)
}

func (d *genjiDB) DeletePlanMetaBeforeTs(ctx context.Context, ts int64) error {
	return nil
}

func (d *genjiDB) ConprofCreateProfileTables(ctx context.Context, id int64) error {
	stmts := []string{
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS conprof_%v_data (ts INTEGER PRIMARY KEY, data BLOB)", id),
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS conprof_%v_meta (ts INTEGER PRIMARY KEY, error TEXT)", id),
	}
	for _, stmt := range stmts {
		if err := d.db.WithContext(ctx).Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func (d *genjiDB) ConprofDeleteProfileTables(ctx context.Context, id int64) error {
	if err := d.db.WithContext(ctx).Exec("DELETE FROM conprof_targets_meta WHERE id = ?", id); err != nil {
		return err
	}
	if err := d.db.WithContext(ctx).Exec(fmt.Sprintf("DROP TABLE IF EXISTS conprof_%v_data", id)); err != nil {
		return err
	}
	if err := d.db.WithContext(ctx).Exec(fmt.Sprintf("DROP TABLE IF EXISTS conprof_%v_meta", id)); err != nil {
		return err
	}
	return nil
}

func (d *genjiDB) ConprofCreateTargetInfo(ctx context.Context, target meta.ProfileTarget, info meta.TargetInfo) error {
	sql := "INSERT INTO conprof_targets_meta (id, kind, component, address, last_scrape_ts) VALUES (?, ?, ?, ?, ?)"
	return d.db.WithContext(ctx).Exec(sql, info.ID, target.Kind, target.Component, target.Address, info.LastScrapeTs)
}

func (d *genjiDB) ConprofUpdateTargetInfo(ctx context.Context, info meta.TargetInfo) error {
	return d.db.WithContext(ctx).Exec("UPDATE conprof_targets_meta set last_scrape_ts = ? where id = ?", info.LastScrapeTs, info.ID)
}

func (d *genjiDB) ConprofQueryTargetInfo(ctx context.Context, target meta.ProfileTarget, f func(info meta.TargetInfo) error) error {
	sql := "SELECT id, last_scrape_ts FROM conprof_targets_meta WHERE kind = ? AND component = ? AND address = ?"
	res, err := d.db.WithContext(ctx).Query(sql, target.Kind, target.Component, target.Address)
	if err != nil {
		return err
	}
	defer res.Close()
	return res.Iterate(func(d types.Document) error {
		info := meta.TargetInfo{}
		err = document.Scan(d, &info.ID, &info.LastScrapeTs)
		if err != nil {
			return err
		}
		return f(info)
	})
}

func (d *genjiDB) ConprofQueryAllProfileTargets(ctx context.Context, f func(target meta.ProfileTarget, info meta.TargetInfo) error) error {
	sql := "SELECT id, kind, component, address, last_scrape_ts FROM conprof_targets_meta"
	res, err := d.db.WithContext(ctx).Query(sql)
	if err != nil {
		return err
	}
	defer res.Close()
	return res.Iterate(func(d types.Document) error {
		info := meta.TargetInfo{}
		target := meta.ProfileTarget{}
		err = document.Scan(d, &info.ID, &target.Kind, &target.Component, &target.Address, &info.LastScrapeTs)
		if err != nil {
			return err
		}
		return f(target, info)
	})
}

func (d *genjiDB) ConprofWriteProfileData(ctx context.Context, id, ts int64, data []byte) error {
	sql := fmt.Sprintf("INSERT INTO conprof_%v_data (ts, data) VALUES (?, ?)", id)
	return d.db.WithContext(ctx).Exec(sql, ts, data)
}

func (d *genjiDB) ConprofQueryProfileData(ctx context.Context, id, begin, end int64, f func(ts int64, data []byte) error) error {
	sql := fmt.Sprintf("SELECT ts, data FROM conprof_%v_data WHERE ts >= ? and ts <= ? ORDER BY ts DESC", id)
	res, err := d.db.WithContext(ctx).Query(sql, begin, end)
	if err != nil {
		return err
	}
	defer res.Close()
	return res.Iterate(func(d types.Document) error {
		var ts int64
		var data []byte
		err = document.Scan(d, &ts, &data)
		if err != nil {
			return err
		}
		return f(ts, data)
	})
}

func (d *genjiDB) ConprofDeleteProfileDataBeforeTs(ctx context.Context, id, ts int64) error {
	sql := fmt.Sprintf("DELETE FROM conprof_%v_data WHERE ts <= ?", id)
	return d.db.WithContext(ctx).Exec(sql, ts)
}

func (d *genjiDB) ConprofWriteProfileMeta(ctx context.Context, id, ts int64, err string) error {
	sql := fmt.Sprintf("INSERT INTO conprof_%v_meta (ts, error) VALUES (?, ?)", id)
	return d.db.WithContext(ctx).Exec(sql, ts, err)
}

func (d *genjiDB) ConprofQueryProfileMeta(ctx context.Context, id, begin, end int64, f func(ts int64, verr string) error) error {
	sql := fmt.Sprintf("SELECT ts, error FROM conprof_%v_meta WHERE ts >= ? and ts <= ? ORDER BY ts DESC", id)
	res, err := d.db.WithContext(ctx).Query(sql, begin, end)
	if err != nil {
		return err
	}
	defer res.Close()
	return res.Iterate(func(d types.Document) error {
		var ts int64
		var verr string
		err = document.Scan(d, &ts, &verr)
		if err != nil {
			return err
		}
		return f(ts, verr)
	})
}

func (d *genjiDB) ConprofDeleteProfileMetaBeforeTs(ctx context.Context, id, ts int64) error {
	sql := fmt.Sprintf("DELETE FROM conprof_%v_meta WHERE ts <= ?", id)
	return d.db.WithContext(ctx).Exec(sql, ts)
}

var (
	lastFlattenTsKey = []byte("last_flatten_ts")
	flattenInterval  = time.Hour * 24
)

func doGCLoop(db *badger.DB, closed chan struct{}) {
	log.Info("badger start to run value log gc loop")
	ticker := time.NewTicker(10 * time.Minute)
	defer func() {
		ticker.Stop()
		log.Info("badger stop running value log gc loop")
	}()

	// run gc when started.
	runGC(db)
	for {
		select {
		case <-ticker.C:
			runGC(db)
		case <-closed:
			return
		}
	}
}

func runGC(db *badger.DB) {
	defer func() {
		r := recover()
		if r != nil {
			log.Error("panic when run badger gc",
				zap.Reflect("r", r),
				zap.Stack("stack trace"))
		}
	}()

	tryFlattenIfNeeded(db)
	runValueLogGC(db)
}

func runValueLogGC(db *badger.DB) {
	// at most do 10 value log gc each time.
	for i := 0; i < 10; i++ {
		err := db.RunValueLogGC(0.1)
		if err != nil {
			if err == badger.ErrNoRewrite {
				log.Info("badger has no value log need gc now")
			} else {
				log.Error("badger run value log gc failed", zap.Error(err))
			}
			return
		}
		log.Info("badger run value log gc success")
	}
}

// tryFlattenIfNeeded try to do flatten if needed.
// Flatten uses to remove the old version keys, otherwise, the value log gc won't release the disk space which occupied
// by the old version keys.
func tryFlattenIfNeeded(db *badger.DB) {
	if !needFlatten(db) {
		return
	}
	err := db.Flatten(runtime.NumCPU()/2 + 1)
	if err != nil {
		log.Error("badger flatten failed", zap.Error(err))
		return
	}
	ts := time.Now().Unix()
	err = storeLastFlattenTs(db, ts)
	if err != nil {
		log.Error("badger store last flatten ts failed", zap.Error(err))
		return
	}
	log.Info("badger flatten success", zap.Int64("ts", ts))
}

func needFlatten(db *badger.DB) bool {
	ts, err := getLastFlattenTs(db)
	if err != nil {
		log.Error("badger get last flatten ts failed", zap.Error(err))
	}
	interval := time.Now().Unix() - ts
	return time.Duration(interval)*time.Second >= flattenInterval
}

func getLastFlattenTs(db *badger.DB) (int64, error) {
	ts := int64(0)
	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(lastFlattenTsKey)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil
			}
			return err
		}
		err = item.Value(func(val []byte) error {
			ts, err = strconv.ParseInt(string(val), 10, 64)
			return err
		})
		return err
	})
	return ts, err
}

func storeLastFlattenTs(db *badger.DB, ts int64) error {
	return db.Update(func(txn *badger.Txn) error {
		v := strconv.FormatInt(ts, 10)
		return txn.Set(lastFlattenTsKey, []byte(v))
	})
}
