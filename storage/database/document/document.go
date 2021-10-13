package document

import (
	"context"
	"log"
	"path"

	"github.com/zhongzc/ng_monitoring_server/config"

	"github.com/dgraph-io/badger/v3"
	"github.com/genjidb/genji"
	"github.com/genjidb/genji/engine/badgerengine"
	"go.uber.org/zap"
)

var documentDB *genji.DB

func Init(cfg *config.Config) {
	dataPath := path.Join(cfg.Storage.Path, "docdb")
	option := badger.DefaultOptions(dataPath)
	l, _ := simpleLogger(&cfg.Log)
	option.Logger = l

	engine, err := badgerengine.NewEngine(option)
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
