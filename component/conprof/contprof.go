package conprof

import (
	"github.com/genjidb/genji"
	"github.com/zhongzc/ng_monitoring/component/conprof/scrape"
	"github.com/zhongzc/ng_monitoring/component/conprof/store"
	"github.com/zhongzc/ng_monitoring/component/topology"
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
