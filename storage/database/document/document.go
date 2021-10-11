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
var l *logger

func Init(logFileName string, logLevel string, dataPath string) {
	var err error

	l, err = simpleLogger(logFileName, logLevel)
	if err != nil {
		// Need to log via the default logger due to `l` is not initialized.
		log.Fatal("Failed to init logger", zap.String("filename", logFileName))
	}

	option := badger.DefaultOptions(dataPath)
	option.Logger = l
	engine, err := badgerengine.NewEngine(option)
	if err != nil {
		l.Fatalf("Failed to open a badger storage, path: %s, err: %v", dataPath, err)
	}

	db, err := genji.New(context.Background(), engine)
	if err != nil {
		l.Fatalf("Failed to open a document database, path: %s, err: %v", dataPath, err)
	}
	documentDB = db
}

func Get() *genji.DB {
	return documentDB
}

func Stop() {
	if err := documentDB.Close(); err != nil {
		l.Fatalf("cannot close the document database, err: %v", err)
	}
}
