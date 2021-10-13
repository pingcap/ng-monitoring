package topsql

import (
	"strings"
	"sync"
)

type MetricSlicePool struct {
	p sync.Pool
}

func (msp *MetricSlicePool) Get() *[]Metric {
	bbv := msp.p.Get()
	if bbv == nil {
		return &[]Metric{}
	}
	return bbv.(*[]Metric)
}

func (msp *MetricSlicePool) Put(bb *[]Metric) {
	*bb = (*bb)[:0]
	msp.p.Put(bb)
}

type StringBuilderPool struct {
	p sync.Pool
}

func (sbp *StringBuilderPool) Get() *strings.Builder {
	sbv := sbp.p.Get()
	if sbv == nil {
		return &strings.Builder{}
	}
	return sbv.(*strings.Builder)
}

func (sbp *StringBuilderPool) Put(sb *strings.Builder) {
	sb.Reset()
	sbp.p.Put(sb)
}

type PrepareSlicePool struct {
	p sync.Pool
}

func (psp *PrepareSlicePool) Get() *[]interface{} {
	psv := psp.p.Get()
	if psv == nil {
		return &[]interface{}{}
	}
	return psv.(*[]interface{})
}

func (psp *PrepareSlicePool) Put(ps *[]interface{}) {
	*ps = (*ps)[:0]
	psp.p.Put(ps)
}
