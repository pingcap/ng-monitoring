package store

import (
	"sync"

	rsmetering "github.com/pingcap/kvproto/pkg/resource_usage_agent"
	"github.com/pingcap/tipb/go-tipb"
)

type Store interface {
	Instances(items []InstanceItem) error
	TopSQLRecord(instance, instanceType string, record *tipb.TopSQLRecord) error
	ResourceMeteringRecord(instance, instanceType string, record *rsmetering.ResourceUsageRecord, schemaInfo *sync.Map) error
	SQLMeta(meta *tipb.SQLMeta) error
	PlanMeta(meta *tipb.PlanMeta) error
	Close()
}
