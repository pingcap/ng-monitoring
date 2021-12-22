package store

import (
	rsmetering "github.com/pingcap/kvproto/pkg/resource_usage_agent"
	"github.com/pingcap/tipb/go-tipb"
)

type Store interface {
	Instance(instance, instanceType string) error
	TopSQLRecord(instance, instanceType string, record *tipb.TopSQLRecord) error
	ResourceMeteringRecord(instance, instanceType string, record *rsmetering.ResourceUsageRecord) error
	SQLMeta(meta *tipb.SQLMeta) error
	PlanMeta(meta *tipb.PlanMeta) error
	Close()
}
