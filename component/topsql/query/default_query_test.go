package query

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKeepTopK(t *testing.T) {
	sqlGroups := []sqlGroup{{
		sqlDigest: "sql0",
		valueSum:  23,
	}, {
		sqlDigest: "sql1",
		valueSum:  4,
	}, {
		sqlDigest: "sql2",
		valueSum:  54,
	}, {
		sqlDigest: "sql3",
		valueSum:  32,
	}, {
		sqlDigest: "sql4",
		valueSum:  0,
	}}

	// k is too large
	groups := sqlGroups
	others := keepTopK(&groups, 20)
	require.Equal(t, groups, sqlGroups)
	require.Nil(t, others)

	// k is too small
	groups = sqlGroups
	others = keepTopK(&groups, 0)
	require.Equal(t, groups, sqlGroups)
	require.Nil(t, others)

	sortedEqual := func(a, b []sqlGroup) {
		require.Equal(t, len(a), len(b))
		sort.Sort(TopKSlice(a))
		sort.Sort(TopKSlice(b))
		require.Equal(t, a, b)
	}

	// keep top 1
	groups = sqlGroups
	others = keepTopK(&groups, 1)
	sortedEqual(groups, []sqlGroup{{
		sqlDigest: "sql2",
		valueSum:  54,
	}})
	sortedEqual(others, []sqlGroup{{
		sqlDigest: "sql0",
		valueSum:  23,
	}, {
		sqlDigest: "sql1",
		valueSum:  4,
	}, {
		sqlDigest: "sql3",
		valueSum:  32,
	}, {
		sqlDigest: "sql4",
		valueSum:  0,
	}})

	// keep top 4
	groups = sqlGroups
	others = keepTopK(&groups, 4)
	sortedEqual(groups, []sqlGroup{{
		sqlDigest: "sql2",
		valueSum:  54,
	}, {
		sqlDigest: "sql0",
		valueSum:  23,
	}, {
		sqlDigest: "sql1",
		valueSum:  4,
	}, {
		sqlDigest: "sql3",
		valueSum:  32,
	}})
	sortedEqual(others, []sqlGroup{{
		sqlDigest: "sql4",
		valueSum:  0,
	}})
}

func TestTopK(t *testing.T) {
	// original metrics
	//
	// sql0:
	//   plan0: ts  1, 2, 3, 4, 5
	//          val 25 10 21 4  1
	//   plan1: ts  1, 3,  4,  5
	//          val 8, 81, 68, 21
	// sql1:
	//   plan0: ts  1,  2,  3,  5
	//          val 65, 38, 75, 20
	// sql2:
	//   plan0: ts  1,  2,  3,  4
	//          val 84, 49, 78, 86
	// sql3:
	//   plan0: ts  1,  2, 3,  4,  5
	//          val 81, 2, 21, 93, 9
	//
	// others:  ts  1,  2,  3,  4, 5
	//          val 14, 79, 96, 48, 68
	metrics := []recordsMetricRespDataResult{{
		Metric: recordsMetricRespDataResultMetric{
			Instance:     "127.0.0.1:10080",
			InstanceType: "tidb",
			SQLDigest:    "sql0",
			PlanDigest:   "plan0",
		},
		Values: []recordsMetricRespDataResultValue{
			{1.0, "25"},
			{2.0, "10"},
			{3.0, "21"},
			{4.0, "4"},
			{5.0, "1"},
		},
	}, {
		Metric: recordsMetricRespDataResultMetric{
			Instance:     "127.0.0.1:10080",
			InstanceType: "tidb",
			SQLDigest:    "sql0",
			PlanDigest:   "plan1",
		},
		Values: []recordsMetricRespDataResultValue{
			{1.0, "8"},
			{3.0, "81"},
			{4.0, "68"},
			{5.0, "21"},
		},
	}, {
		Metric: recordsMetricRespDataResultMetric{
			Instance:     "127.0.0.1:10080",
			InstanceType: "tidb",
			SQLDigest:    "sql1",
			PlanDigest:   "plan0",
		},
		Values: []recordsMetricRespDataResultValue{
			{1.0, "65"},
			{2.0, "38"},
			{3.0, "75"},
			{5.0, "20"},
		},
	}, {
		Metric: recordsMetricRespDataResultMetric{
			Instance:     "127.0.0.1:10080",
			InstanceType: "tidb",
			SQLDigest:    "sql2",
			PlanDigest:   "plan0",
		},
		Values: []recordsMetricRespDataResultValue{
			{1.0, "84"},
			{2.0, "49"},
			{3.0, "78"},
			{4.0, "86"},
		},
	}, {
		Metric: recordsMetricRespDataResultMetric{
			Instance:     "127.0.0.1:10080",
			InstanceType: "tidb",
			SQLDigest:    "sql3",
			PlanDigest:   "plan0",
		},
		Values: []recordsMetricRespDataResultValue{
			{1.0, "81"},
			{2.0, "2"},
			{3.0, "21"},
			{4.0, "93"},
			{5.0, "9"},
		},
	}, {
		Metric: recordsMetricRespDataResultMetric{
			Instance:     "127.0.0.1:10080",
			InstanceType: "tidb",
			SQLDigest:    "",
			PlanDigest:   "",
		},
		Values: []recordsMetricRespDataResultValue{
			{1.0, "14"},
			{2.0, "79"},
			{3.0, "96"},
			{4.0, "48"},
			{5.0, "68"},
		},
	}}

	// top 10
	//
	// sql0:
	//   plan0: ts  1, 2, 3, 4, 5
	//          val 25 10 21 4  1
	//   plan1: ts  1, 3,  4,  5
	//          val 8, 81, 68, 21
	// sql1:
	//   plan0: ts  1,  2,  3,  5
	//          val 65, 38, 75, 20
	// sql2:
	//   plan0: ts  1,  2,  3,  4
	//          val 84, 49, 78, 86
	// sql3:
	//   plan0: ts  1,  2, 3,  4,  5
	//          val 81, 2, 21, 93, 9
	//
	// others:  ts  1,  2,  3,  4, 5
	//          val 14, 79, 96, 48, 68
	var sqlGroups []sqlGroup
	topK(metrics, 10, &sqlGroups)
	sort.Sort(sqlGroupSlice(sqlGroups))
	require.Equal(t, sqlGroups, []sqlGroup{{
		sqlDigest: "",
		planSeries: []planSeries{{
			planDigest:    "",
			timestampSecs: []uint64{1, 2, 3, 4, 5},
			values:        []uint64{14, 79, 96, 48, 68},
		}},
		valueSum: 305,
	}, {
		sqlDigest: "sql0",
		planSeries: []planSeries{{
			planDigest:    "plan0",
			timestampSecs: []uint64{1, 2, 3, 4, 5},
			values:        []uint64{25, 10, 21, 4, 1},
		}, {
			planDigest:    "plan1",
			timestampSecs: []uint64{1, 3, 4, 5},
			values:        []uint64{8, 81, 68, 21},
		}},
		valueSum: 239,
	}, {
		sqlDigest: "sql1",
		planSeries: []planSeries{{
			planDigest:    "plan0",
			timestampSecs: []uint64{1, 2, 3, 5},
			values:        []uint64{65, 38, 75, 20},
		}},
		valueSum: 198,
	}, {
		sqlDigest: "sql2",
		planSeries: []planSeries{{
			planDigest:    "plan0",
			timestampSecs: []uint64{1, 2, 3, 4},
			values:        []uint64{84, 49, 78, 86},
		}},
		valueSum: 297,
	}, {
		sqlDigest: "sql3",
		planSeries: []planSeries{{
			planDigest:    "plan0",
			timestampSecs: []uint64{1, 2, 3, 4, 5},
			values:        []uint64{81, 2, 21, 93, 9},
		}},
		valueSum: 206,
	}})

	// top 3
	//
	// sql0:
	//   plan0: ts  1, 2, 3, 4, 5
	//          val 25 10 21 4  1
	//   plan1: ts  1, 3,  4,  5
	//          val 8, 81, 68, 21
	// sql2:
	//   plan0: ts  1,  2,  3,  4
	//          val 84, 49, 78, 86
	// sql3:
	//   plan0: ts  1,  2, 3,  4,  5
	//          val 81, 2, 21, 93, 9
	//
	// others:  ts  1,  2,   3,   4,  5
	//          val 79, 117, 171, 48, 88
	sqlGroups = nil
	topK(metrics, 3, &sqlGroups)
	sort.Sort(sqlGroupSlice(sqlGroups))
	require.Equal(t, sqlGroups, []sqlGroup{{
		sqlDigest: "",
		planSeries: []planSeries{{
			planDigest:    "",
			timestampSecs: []uint64{1, 2, 3, 4, 5},
			values:        []uint64{79, 117, 171, 48, 88},
		}},
	}, {
		sqlDigest: "sql0",
		planSeries: []planSeries{{
			planDigest:    "plan0",
			timestampSecs: []uint64{1, 2, 3, 4, 5},
			values:        []uint64{25, 10, 21, 4, 1},
		}, {
			planDigest:    "plan1",
			timestampSecs: []uint64{1, 3, 4, 5},
			values:        []uint64{8, 81, 68, 21},
		}},
		valueSum: 239,
	}, {
		sqlDigest: "sql2",
		planSeries: []planSeries{{
			planDigest:    "plan0",
			timestampSecs: []uint64{1, 2, 3, 4},
			values:        []uint64{84, 49, 78, 86},
		}},
		valueSum: 297,
	}, {
		sqlDigest: "sql3",
		planSeries: []planSeries{{
			planDigest:    "plan0",
			timestampSecs: []uint64{1, 2, 3, 4, 5},
			values:        []uint64{81, 2, 21, 93, 9},
		}},
		valueSum: 206,
	}})

	// top 1
	//
	// sql2:
	//   plan0: ts  1,  2,  3,  4
	//          val 84, 49, 78, 86
	//
	// others:  ts  1,   2,   3,   4,   5
	//          val 193, 129, 294, 213, 119
	sqlGroups = nil
	topK(metrics, 1, &sqlGroups)
	sort.Sort(sqlGroupSlice(sqlGroups))
	require.Equal(t, sqlGroups, []sqlGroup{{
		sqlDigest: "",
		planSeries: []planSeries{{
			planDigest:    "",
			timestampSecs: []uint64{1, 2, 3, 4, 5},
			values:        []uint64{193, 129, 294, 213, 119},
		}},
	}, {
		sqlDigest: "sql2",
		planSeries: []planSeries{{
			planDigest:    "plan0",
			timestampSecs: []uint64{1, 2, 3, 4},
			values:        []uint64{84, 49, 78, 86},
		}},
		valueSum: 297,
	}})

	sqlGroups = nil
	topK(nil, 10, &sqlGroups)
	require.Nil(t, sqlGroups)
}

type sqlGroupSlice []sqlGroup

var _ sort.Interface = sqlGroupSlice{}

func (s sqlGroupSlice) Len() int           { return len(s) }
func (s sqlGroupSlice) Less(i, j int) bool { return s[i].sqlDigest < s[j].sqlDigest }
func (s sqlGroupSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
