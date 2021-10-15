package continuousprofiling

import (
	"github.com/genjidb/genji"
	"github.com/zhongzc/ng_monitoring/component/continuousprofiling/scrape"
	"github.com/zhongzc/ng_monitoring/component/continuousprofiling/store"
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
