package document

import (
	"context"
	"path"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/genjidb/genji"
	"github.com/genjidb/genji/engine/badgerengine"
	"github.com/pingcap/log"
	"github.com/pingcap/ng_monitoring/config"
	"github.com/pingcap/ng_monitoring/utils"
	"go.uber.org/zap"
)

var documentDB *genji.DB
var closeCh chan struct{}

func Init(cfg *config.Config) {
	dataPath := path.Join(cfg.Storage.Path, "docdb")
	l, _ := simpleLogger(&cfg.Log)
	opts := badger.DefaultOptions(dataPath).
		WithZSTDCompressionLevel(3).
		WithBlockSize(8 * 1024).
		WithValueThreshold(128 * 1024).
		WithLogger(l)

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

func doGCLoop(db *badger.DB, closed chan struct{}) {
	log.Info("badger start to run value log gc loop")
	ticker := time.NewTicker(10 * time.Minute)
	defer func() {
		ticker.Stop()
		log.Info("badger stop running value log gc loop")
	}()
	for {
		select {
		case <-ticker.C:
			runValueLogGC(db)
		case <-closed:
			return
		}
	}
}

func runValueLogGC(db *badger.DB) {
	defer func() {
		r := recover()
		if r != nil {
			log.Error("panic when run badger value log",
				zap.Reflect("r", r),
				zap.Stack("stack trace"))
		}
	}()

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

func Get() *genji.DB {
	return documentDB
}

func Stop() {
	close(closeCh)
	if err := documentDB.Close(); err != nil {
		log.Fatal("failed to close the document database", zap.Error(err))
	}
}
