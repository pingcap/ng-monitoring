package document

import (
	"context"
	"flag"

	"github.com/dgraph-io/badger/v3"
	"github.com/genjidb/genji"
	"github.com/genjidb/genji/engine/badgerengine"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

var (
	// docStorePath is a path to storage document data, i.e. sql digest and plan digest.
	docDBPath = flag.String("docDBPath", "doc-data", "Path to storage document data")
)

var documentDB *genji.DB

func Init() {
	var err error
	engine, err := badgerengine.NewEngine(badger.DefaultOptions(*docDBPath))
	if err != nil {
		log.Fatal("cannot open a badger storage", zap.String("path", *docDBPath), zap.Error(err))
	}

	db, err := genji.New(context.Background(), engine)
	if err != nil {
		log.Fatal("cannot open a document database", zap.String("path", *docDBPath), zap.Error(err))
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
