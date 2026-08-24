package store

import (
	"testing"

	rsmetering "github.com/pingcap/kvproto/pkg/resource_usage_agent"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestRSMeteringProtoToMetricsBlockReadCount(t *testing.T) {
	tagBytes, err := (&tipb.ResourceGroupTag{
		SqlDigest:  []byte("sql-digest"),
		PlanDigest: []byte("plan-digest"),
		TableId:    1,
	}).Marshal()
	require.NoError(t, err)

	testCases := []struct {
		name       string
		values     []uint64
		wantMetric bool
	}{
		{name: "all zero", values: []uint64{0, 0}, wantMetric: false},
		{name: "contains nonzero", values: []uint64{0, 5}, wantMetric: true},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			items := make([]*rsmetering.GroupTagRecordItem, 0, len(testCase.values))
			for i, value := range testCase.values {
				items = append(items, &rsmetering.GroupTagRecordItem{
					TimestampSec:          uint64(i + 1),
					RocksdbBlockReadCount: value,
				})
			}

			metrics, err := rsMeteringProtoToMetrics("tikv-0", "tikv", &rsmetering.ResourceUsageRecord{
				RecordOneof: &rsmetering.ResourceUsageRecord_Record{Record: &rsmetering.GroupTagRecord{
					ResourceGroupTag: tagBytes,
					Items:            items,
				}},
			}, nil)
			require.NoError(t, err)

			metric := findMetric(metrics, MetricNameRocksdbBlockReadCount)
			if !testCase.wantMetric {
				require.Nil(t, metric)
				require.Len(t, metrics, 9)
				return
			}
			require.NotNil(t, metric)
			require.Equal(t, []uint64{1000, 2000}, metric.TimestampMs)
			require.Equal(t, testCase.values, metric.Values)
			require.Len(t, metrics, 10)
		})
	}
}

func TestRSMeteringRegionProtoToMetricsBlockReadCount(t *testing.T) {
	testCases := []struct {
		name       string
		values     []uint64
		wantMetric bool
	}{
		{name: "all zero", values: []uint64{0, 0}, wantMetric: false},
		{name: "contains nonzero", values: []uint64{0, 5}, wantMetric: true},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			items := make([]*rsmetering.GroupTagRecordItem, 0, len(testCase.values))
			for i, value := range testCase.values {
				items = append(items, &rsmetering.GroupTagRecordItem{
					TimestampSec:          uint64(i + 1),
					RocksdbBlockReadCount: value,
				})
			}

			metrics := rsMeteringRegionProtoToMetrics("tikv-0", "tikv", &rsmetering.RegionRecord{
				RegionId: 1,
				Items:    items,
			})

			metric := findMetric(metrics, MetricNameRegionRocksdbBlockReadCount)
			if !testCase.wantMetric {
				require.Nil(t, metric)
				require.Len(t, metrics, 7)
				return
			}
			require.NotNil(t, metric)
			require.Equal(t, []uint64{1000, 2000}, metric.TimestampMs)
			require.Equal(t, testCase.values, metric.Values)
			require.Len(t, metrics, 8)
		})
	}
}

func findMetric(metrics []Metric, name string) *Metric {
	for i := range metrics {
		switch tags := metrics[i].Metric.(type) {
		case recordTags:
			if tags.Name == name {
				return &metrics[i]
			}
		case regionTags:
			if tags.Name == name {
				return &metrics[i]
			}
		}
	}
	return nil
}
