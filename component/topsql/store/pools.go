package store

import (
	"strings"
	"sync"
)

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
