package query

import "sync"

type metricRespPool struct {
    p sync.Pool
}

func (mrp *metricRespPool) Get() *metricResp {
    mrv := mrp.p.Get()
    if mrv == nil {
        return &metricResp{}
    }
    return mrv.(*metricResp)
}

func (mrp *metricRespPool) Put(mrv *metricResp) {
    mrv.Status = ""
    mrv.Data.ResultType = ""
    mrv.Data.Results = mrv.Data.Results[:0]
    mrp.p.Put(mrv)
}
