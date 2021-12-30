package query

import (
	"sync"
)

type recordsRespPool struct {
	p sync.Pool
}

func (rrp *recordsRespPool) Get() *recordsMetricResp {
	rrv := rrp.p.Get()
	if rrv == nil {
		return &recordsMetricResp{}
	}
	return rrv.(*recordsMetricResp)
}

func (rrp *recordsRespPool) Put(rrv *recordsMetricResp) {
	rrv.Status = ""
	rrv.Data.ResultType = ""
	rrv.Data.Results = rrv.Data.Results[:0]
	rrp.p.Put(rrv)
}

type sqlGroupSlicePool struct {
	p sync.Pool
}

func (ssp *sqlGroupSlicePool) Get() *[]sqlGroup {
	ssv := ssp.p.Get()
	if ssv == nil {
		return &[]sqlGroup{}
	}
	return ssv.(*[]sqlGroup)
}

func (ssp *sqlGroupSlicePool) Put(ssv *[]sqlGroup) {
	*ssv = (*ssv)[:0]
	ssp.p.Put(ssv)
}

type sqlDigestMapPool struct {
	p sync.Pool
}

func (smp *sqlDigestMapPool) Get() map[string]sqlGroup {
	smv := smp.p.Get()
	if smv == nil {
		return make(map[string]sqlGroup)
	}
	return smv.(map[string]sqlGroup)
}

func (smp *sqlDigestMapPool) Put(smv map[string]sqlGroup) {
	for key := range smv {
		delete(smv, key)
	}
	smp.p.Put(smv)
}

type sumMapPool struct {
	p sync.Pool
}

func (smp *sumMapPool) Get() map[RecordKey]float64 {
	smv := smp.p.Get()
	if smv == nil {
		return make(map[RecordKey]float64)
	}
	return smv.(map[RecordKey]float64)
}

func (smp *sumMapPool) Put(smv map[RecordKey]float64) {
	for key := range smv {
		delete(smv, key)
	}
	smp.p.Put(smv)
}
