package document

import (
	"context"
	"path"

	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/ng-monitoring/utils"

	"github.com/dgraph-io/badger/v3"
	"github.com/genjidb/genji"
	"github.com/genjidb/genji/engine/badgerengine"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

var documentDB *genji.DB
var closeCh chan struct{}

func Init(cfg *config.Config) {
	badger.DefaultIteratorOptions.PrefetchValues = false

	dataPath := path.Join(cfg.Storage.Path, "docdb")
	l, _ := initLogger(cfg)
	var opts badger.Options
	if cfg.DocDB.LSMOnly {
		opts = badger.LSMOnlyOptions(dataPath)
	} else {
		opts = badger.DefaultOptions(dataPath)
	}
	if !cfg.DocDB.LSMOnly {
		opts = opts.WithValueThreshold(cfg.DocDB.ValueThreshold)
	}
	opts = opts.
		WithLogger(l).
		WithSyncWrites(cfg.DocDB.SyncWrites).
		WithNumVersionsToKeep(cfg.DocDB.NumVersionsToKeep).
		WithNumGoroutines(cfg.DocDB.NumGoroutines).
		WithMemTableSize(cfg.DocDB.MemTableSize).
		WithBaseTableSize(cfg.DocDB.BaseTableSize).
		WithBaseLevelSize(cfg.DocDB.BaseLevelSize).
		WithLevelSizeMultiplier(cfg.DocDB.LevelSizeMultiplier).
		WithMaxLevels(cfg.DocDB.MaxLevels).
		WithVLogPercentile(cfg.DocDB.VLogPercentile).
		WithNumMemtables(cfg.DocDB.NumMemtables).
		WithBlockSize(cfg.DocDB.BlockSize).
		WithBloomFalsePositive(cfg.DocDB.BloomFalsePositive).
		WithBlockCacheSize(cfg.DocDB.BlockCacheSize).
		WithIndexCacheSize(cfg.DocDB.IndexCacheSize).
		WithNumLevelZeroTables(cfg.DocDB.NumLevelZeroTables).
		WithNumLevelZeroTablesStall(cfg.DocDB.NumLevelZeroTablesStall).
		WithValueLogFileSize(cfg.DocDB.ValueLogFileSize).
		WithValueLogMaxEntries(cfg.DocDB.ValueLogMaxEntries).
		WithNumCompactors(cfg.DocDB.NumCompactors).
		WithZSTDCompressionLevel(cfg.DocDB.ZSTDCompressionLevel)

	engine, err := badgerengine.NewEngine(opts)
	if err != nil {
		log.Fatal("failed to open a badger storage", zap.String("path", dataPath), zap.Error(err))
	}

	closeCh = make(chan struct{})
	go utils.GoWithRecovery(func() {
		doGCLoop(engine.DB, closeCh)
	}, nil)

	db, err := genji.New(context.Background(), engine)
	if err != nil {
		log.Fatal("failed to open a document database", zap.String("path", dataPath), zap.Error(err))
	}
	documentDB = db
}

func Get() *genji.DB {
	return documentDB
}

func Stop() {
	close(closeCh)
	if err := documentDB.Close(); err != nil {
		log.Fatal("failed to close the document database", zap.Error(err))
	}
}
