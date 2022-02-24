package document

import (
	"runtime"
	"strconv"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

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
