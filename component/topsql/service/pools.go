package service

import (
	"sync"

	"github.com/pingcap/ng-monitoring/component/topsql/query"

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

func (rsp *ResourceCPUTimeSlicePool) Get() *[]*resource_usage_agent.ResourceUsageRecord {
	rs := rsp.p.Get()
	if rs == nil {
		return &[]*resource_usage_agent.ResourceUsageRecord{}
	}
	return rs.(*[]*resource_usage_agent.ResourceUsageRecord)
}

func (rsp *ResourceCPUTimeSlicePool) Put(rs *[]*resource_usage_agent.ResourceUsageRecord) {
	*rs = (*rs)[:0]
	rsp.p.Put(rs)
}

type recordsPool struct {
	p sync.Pool
}

func (tip *recordsPool) Get() *[]query.RecordItem {
	tiv := tip.p.Get()
	if tiv == nil {
		return &[]query.RecordItem{}
	}
	return tiv.(*[]query.RecordItem)
}

func (tip *recordsPool) Put(ti *[]query.RecordItem) {
	*ti = (*ti)[:0]
	tip.p.Put(ti)
}

type summarySQLPool struct {
	p sync.Pool
}

func (sp *summarySQLPool) Get() *[]query.SummaryItem {
	sv := sp.p.Get()
	if sv == nil {
		return &[]query.SummaryItem{}
	}
	return sv.(*[]query.SummaryItem)
}

func (sp *summarySQLPool) Put(s *[]query.SummaryItem) {
	*s = (*s)[:0]
	sp.p.Put(s)
}

type summaryByItemPool struct {
	p sync.Pool
}

func (tp *summaryByItemPool) Get() *[]query.SummaryByItem {
	tv := tp.p.Get()
	if tv == nil {
		return &[]query.SummaryByItem{}
	}
	return tv.(*[]query.SummaryByItem)
}

func (tp *summaryByItemPool) Put(t *[]query.SummaryByItem) {
	*t = (*t)[:0]
	tp.p.Put(t)
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
