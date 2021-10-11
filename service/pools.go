package service

import (
	"sync"

	"github.com/zhongzc/diag_backend/storage/query"

	"github.com/pingcap/kvproto/pkg/resource_usage_agent"
	"github.com/pingcap/tipb/go-tipb"
)

type SQLMetaSlicePool struct {
	p sync.Pool
}

func (ssp *SQLMetaSlicePool) Get() *[]*tipb.SQLMeta {
	ssv := ssp.p.Get()
	if ssv == nil {
		return &[]*tipb.SQLMeta{}
	}
	return ssv.(*[]*tipb.SQLMeta)
}

func (ssp *SQLMetaSlicePool) Put(ss *[]*tipb.SQLMeta) {
	*ss = (*ss)[:0]
	ssp.p.Put(ss)
}

type PlanMetaSlicePool struct {
	p sync.Pool
}

func (psp *PlanMetaSlicePool) Get() *[]*tipb.PlanMeta {
	ps := psp.p.Get()
	if ps == nil {
		return &[]*tipb.PlanMeta{}
	}
	return ps.(*[]*tipb.PlanMeta)
}

func (psp *PlanMetaSlicePool) Put(ps *[]*tipb.PlanMeta) {
	*ps = (*ps)[:0]
	psp.p.Put(ps)
}

type ResourceCPUTimeSlicePool struct {
	p sync.Pool
}

func (rsp *ResourceCPUTimeSlicePool) Get() *[]*resource_usage_agent.CPUTimeRecord {
	rs := rsp.p.Get()
	if rs == nil {
		return &[]*resource_usage_agent.CPUTimeRecord{}
	}
	return rs.(*[]*resource_usage_agent.CPUTimeRecord)
}

func (rsp *ResourceCPUTimeSlicePool) Put(rs *[]*resource_usage_agent.CPUTimeRecord) {
	*rs = (*rs)[:0]
	rsp.p.Put(rs)
}

type TopSQLItemsPool struct {
	p sync.Pool
}

func (tip *TopSQLItemsPool) Get() *[]query.TopSQLItem {
	tiv := tip.p.Get()
	if tiv == nil {
		return &[]query.TopSQLItem{}
	}
	return tiv.(*[]query.TopSQLItem)
}

func (tip *TopSQLItemsPool) Put(ti *[]query.TopSQLItem) {
	*ti = (*ti)[:0]
	tip.p.Put(ti)
}

type InstanceItemsPool struct {
	p sync.Pool
}

func (iip *InstanceItemsPool) Get() *[]query.InstanceItem {
	iiv := iip.p.Get()
	if iiv == nil {
		return &[]query.InstanceItem{}
	}
	return iiv.(*[]query.InstanceItem)
}

func (iip *InstanceItemsPool) Put(iiv *[]query.InstanceItem) {
	*iiv = (*iiv)[:0]
	iip.p.Put(iiv)
}
