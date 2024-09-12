package mock

import (
	"sync"
	"time"

	"github.com/pingcap/ng-monitoring/component/topsql/store"

	rsmetering "github.com/pingcap/kvproto/pkg/resource_usage_agent"
	"github.com/pingcap/tipb/go-tipb"
)

type Component struct {
	Name string
	Addr string
}

type MemStore struct {
	sync.Mutex

	// instance -> value
	InstanceStore map[Component]struct{}

	// instance -> sql digest -> plan digest -> records
	TopSQLRecords map[string]map[string]map[string]*tipb.TopSQLRecord

	// instance -> resource tag -> records
	ResourceMeteringRecords map[string]map[string]*rsmetering.ResourceUsageRecord

	// SQL digest -> meta
	SQLMetas map[string]struct {
		Meta *tipb.SQLMeta
	}

	// plan digest -> meta
	PlanMetas map[string]struct {
		Meta *tipb.PlanMeta
	}
}

func NewMemStore() *MemStore {
	return &MemStore{
		InstanceStore:           make(map[Component]struct{}),
		TopSQLRecords:           make(map[string]map[string]map[string]*tipb.TopSQLRecord),
		ResourceMeteringRecords: make(map[string]map[string]*rsmetering.ResourceUsageRecord),
		SQLMetas: make(map[string]struct {
			Meta *tipb.SQLMeta
		}),
		PlanMetas: make(map[string]struct {
			Meta *tipb.PlanMeta
		}),
	}
}

func (m *MemStore) Pred(pred func(*MemStore) bool, beginWaitTime time.Duration, maxWaitTime time.Duration) bool {
	begin := time.Now()
	timeToWait := beginWaitTime

	for {
		passed := func() bool {
			m.Lock()
			defer m.Unlock()

			return pred(m)
		}()

		waitedTime := time.Since(begin)
		if passed {
			return true
		} else if waitedTime >= maxWaitTime {
			return false
		}

		if waitedTime+timeToWait > maxWaitTime {
			timeToWait = maxWaitTime - waitedTime
		}
		time.Sleep(timeToWait)
		timeToWait *= 2
	}
}

var _ store.Store = &MemStore{}

func (m *MemStore) Instances(items []store.InstanceItem) error {
	m.Lock()
	for _, item := range items {
		m.InstanceStore[Component{
			Name: item.InstanceType,
			Addr: item.Instance,
		}] = struct{}{}
	}
	m.Unlock()

	return nil
}

func (m *MemStore) TopSQLRecord(instance, _ string, record *tipb.TopSQLRecord) error {
	m.Lock()
	defer m.Unlock()

	if _, ok := m.TopSQLRecords[instance]; !ok {
		m.TopSQLRecords[instance] = make(map[string]map[string]*tipb.TopSQLRecord)
	}
	if _, ok := m.TopSQLRecords[instance][string(record.SqlDigest)]; !ok {
		m.TopSQLRecords[instance][string(record.SqlDigest)] = make(map[string]*tipb.TopSQLRecord)
	}
	if _, ok := m.TopSQLRecords[instance][string(record.SqlDigest)][string(record.PlanDigest)]; !ok {
		m.TopSQLRecords[instance][string(record.SqlDigest)][string(record.PlanDigest)] = &tipb.TopSQLRecord{
			SqlDigest:  record.SqlDigest,
			PlanDigest: record.PlanDigest,
		}
	}
	r := m.TopSQLRecords[instance][string(record.SqlDigest)][string(record.PlanDigest)]
	r.Items = append(r.Items, record.Items...)

	return nil
}

func (m *MemStore) ResourceMeteringRecord(instance, _ string, record *rsmetering.ResourceUsageRecord, _ *sync.Map) error {
	m.Lock()
	defer m.Unlock()
	if _, ok := m.ResourceMeteringRecords[instance]; !ok {
		m.ResourceMeteringRecords[instance] = make(map[string]*rsmetering.ResourceUsageRecord)
	}
	if _, ok := m.ResourceMeteringRecords[instance][string(record.GetRecord().ResourceGroupTag)]; !ok {
		m.ResourceMeteringRecords[instance][string(record.GetRecord().ResourceGroupTag)] = &rsmetering.ResourceUsageRecord{
			RecordOneof: &rsmetering.ResourceUsageRecord_Record{
				Record: &rsmetering.GroupTagRecord{
					ResourceGroupTag: record.GetRecord().ResourceGroupTag,
				},
			},
		}
	}
	r := m.ResourceMeteringRecords[instance][string(record.GetRecord().ResourceGroupTag)]
	r.GetRecord().Items = append(r.GetRecord().Items, record.GetRecord().GetItems()...)

	return nil
}

func (m *MemStore) SQLMeta(meta *tipb.SQLMeta) error {
	m.Lock()
	m.SQLMetas[string(meta.SqlDigest)] = struct{ Meta *tipb.SQLMeta }{Meta: meta}
	m.Unlock()

	return nil
}

func (m *MemStore) PlanMeta(meta *tipb.PlanMeta) error {
	m.Lock()
	m.PlanMetas[string(meta.PlanDigest)] = struct{ Meta *tipb.PlanMeta }{Meta: meta}
	m.Unlock()

	return nil
}

func (m *MemStore) Close() {
}
