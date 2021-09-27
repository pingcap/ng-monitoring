package document

import (
	"context"

	"github.com/dgraph-io/badger/v3"
	"github.com/genjidb/genji"
	"github.com/genjidb/genji/engine/badgerengine"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

var documentDB *genji.DB

func Init(path string) {
	var err error
	engine, err := badgerengine.NewEngine(badger.DefaultOptions(path))
	if err != nil {
		log.Fatal("cannot open a badger storage", zap.String("path", path), zap.Error(err))
	}

	db, err := genji.New(context.Background(), engine)
	if err != nil {
		log.Fatal("cannot open a document database", zap.String("path", path), zap.Error(err))
	}
	documentDB = db
}

func Get() *genji.DB {
	return documentDB
}

func Stop() {
	if err := documentDB.Close(); err != nil {
		log.Fatal("cannot close the document database", zap.Error(err))
	}
}
