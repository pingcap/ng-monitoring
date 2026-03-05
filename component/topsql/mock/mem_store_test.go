package mock

import (
	"testing"

	rsmetering "github.com/pingcap/kvproto/pkg/resource_usage_agent"
	"github.com/stretchr/testify/require"
)

func TestMemStoreResourceMeteringRecordIgnoreRegionRecord(t *testing.T) {
	ms := NewMemStore()
	record := &rsmetering.ResourceUsageRecord{
		RecordOneof: &rsmetering.ResourceUsageRecord_RegionRecord{
			RegionRecord: &rsmetering.RegionRecord{
				RegionId: 123,
			},
		},
	}

	require.NotPanics(t, func() {
		require.NoError(t, ms.ResourceMeteringRecord("127.0.0.1:20160", "tikv", record, nil))
	})

	_, ok := ms.ResourceMeteringRecords["127.0.0.1:20160"]
	require.False(t, ok)
}
