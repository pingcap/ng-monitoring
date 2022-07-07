package conprof

import (
	"github.com/pingcap/ng-monitoring/component/conprof/scrape"
	"github.com/pingcap/ng-monitoring/component/conprof/store"
	"github.com/pingcap/ng-monitoring/component/topology"

	"github.com/genjidb/genji"
)

var (
	storage *store.ProfileStorage
	manager *scrape.Manager
)

func Init(db *genji.DB, subscriber topology.Subscriber) error {
	var err error
	storage, err = store.NewProfileStorage(db)
	if err != nil {
		return err
	}
	manager = scrape.NewManager(storage, subscriber)
	manager.Start()
	return nil
}

func Stop() {
	manager.Close()
}

func GetStorage() *store.ProfileStorage {
	return storage
}

func GetManager() *scrape.Manager {
	return manager
}
