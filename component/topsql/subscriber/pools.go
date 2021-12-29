package subscriber

import (
	"sync"

	"github.com/pingcap/ng-monitoring/component/topsql/store"
)

type instancesItemSlicePool struct {
	p sync.Pool
}

func (isp *instancesItemSlicePool) Get() *[]store.InstanceItem {
	is := isp.p.Get()
	if is == nil {
		return &[]store.InstanceItem{}
	}
	return is.(*[]store.InstanceItem)
}

func (isp *instancesItemSlicePool) Put(is *[]store.InstanceItem) {
	*is = (*is)[:0]
	isp.p.Put(is)
}
