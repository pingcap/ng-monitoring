package document

import (
	"context"
	"github.com/dgraph-io/badger/v3/options"
	"log"
	"path"

	"github.com/zhongzc/ng_monitoring/config"

	"github.com/dgraph-io/badger/v3"
	"github.com/genjidb/genji"
	"github.com/genjidb/genji/engine/badgerengine"
	"go.uber.org/zap"
)

var documentDB *genji.DB

func Init(cfg *config.Config) {
	dataPath := path.Join(cfg.Storage.Path, "docdb")
	l, _ := simpleLogger(&cfg.Log)
	opts := badger.DefaultOptions(dataPath).
		WithCompression(options.ZSTD).
		WithZSTDCompressionLevel(3).
		WithBlockSize(8 * 1024 * 1024).
		WithValueThreshold(8 * 1024 * 1024).
		WithLogger(l)

	engine, err := badgerengine.NewEngine(opts)
	if err != nil {
		log.Fatal("Failed to open a badger storage", zap.String("path", dataPath), zap.Error(err))
	}

	db, err := genji.New(context.Background(), engine)
	if err != nil {
		log.Fatal("Failed to open a document database", zap.String("path", dataPath), zap.Error(err))
	}
	documentDB = db
}

func Get() *genji.DB {
	return documentDB
}

func Stop() {
	if err := documentDB.Close(); err != nil {
		log.Fatal("Failed to close the document database", zap.Error(err))
	}
}
