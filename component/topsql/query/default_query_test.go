package query

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/ng-monitoring/component/topsql/store"

	"github.com/stretchr/testify/require"
)

func TestNormalizeOrderBy(t *testing.T) {
	testCases := []struct {
		input    string
		expected string
	}{
		{"", ""},
		{" CPU ", OrderByCPU},
		{"LogicalIO", OrderByLogicalIO},
		{"LOGICAL_READ", OrderByLogicalRead},
		{"logical_write", OrderByLogicalWrite},
		{"BLOCK_READ", OrderByBlockRead},
	}

	for _, testCase := range testCases {
		require.Equal(t, testCase.expected, NormalizeOrderBy(testCase.input))
	}
}

func TestBuildDetailedIOSummaryQueries(t *testing.T) {
	const (
		instance     = "127.0.0.1:20160"
		instanceType = "tikv"
		windowSecs   = 60
	)

	testCases := []struct {
		orderBy          string
		metricName       string
		regionMetricName string
	}{
		{OrderByLogicalRead, store.MetricNameLogicalReadBytes, store.MetricNameRegionLogicalReadBytes},
		{OrderByLogicalWrite, store.MetricNameLogicalWriteBytes, store.MetricNameRegionLogicalWriteBytes},
		{OrderByBlockRead, store.MetricNameRocksdbBlockReadCount, store.MetricNameRegionRocksdbBlockReadCount},
	}

	for _, testCase := range testCases {
		t.Run(testCase.orderBy, func(t *testing.T) {
			query, err := buildOrderBySummaryQuery(testCase.orderBy, windowSecs, instance, instanceType)
			require.NoError(t, err)
			require.Equal(t, "sum_over_time("+testCase.metricName+"{instance=\"127.0.0.1:20160\", instance_type=\"tikv\"}[60])", query)

			query, err = buildOrderBySummaryByQuery(testCase.orderBy, windowSecs, instance, instanceType, AggLevelTable)
			require.NoError(t, err)
			require.Equal(t, "sum(sum_over_time("+testCase.metricName+"{instance=\"127.0.0.1:20160\", instance_type=\"tikv\"}[60])) by (table)", query)

			query, err = buildOrderBySummaryByQuery(testCase.orderBy, windowSecs, instance, instanceType, AggByRegionID)
			require.NoError(t, err)
			require.Equal(t, "sum(sum_over_time("+testCase.regionMetricName+"{instance=\"127.0.0.1:20160\", instance_type=\"tikv\"}[60])) by (region_id)", query)
		})
	}
}

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

// digestLabel returns a digest whose VM label form (hex-encoded by the store) is 64
// characters, matching real TiDB SQL digests.
func digestLabel(i int) string {
	return fmt.Sprintf("%064x", i)
}

func TestSplitTopSQLDigestsByQueryLen(t *testing.T) {
	const instance = "127.0.0.1:20160"
	const instanceType = "tikv"

	buildNetworkTops := func(matcher string) (string, error) {
		return buildTopSummaryBatchQuery(OrderByNetwork, 6, instance, instanceType, matcher)
	}
	buildCPUTops := func(matcher string) (string, error) {
		return buildTopSummaryBatchQuery(OrderByCPU, 6, instance, instanceType, matcher)
	}

	t.Run("partition and budget", func(t *testing.T) {
		digests := make([]string, 500)
		for i := range digests {
			digests[i] = digestLabel(i)
		}
		batches, err := splitTopSQLDigests(digests, buildNetworkTops)
		require.NoError(t, err)
		require.Greater(t, len(batches), 1)

		seen := make(map[string]int)
		for _, batch := range batches {
			require.NotEmpty(t, batch)
			queryStr, err := buildTopSummaryBatchQuery(OrderByNetwork, 6, instance, instanceType, ", sql_digest=~"+sqlDigestRegexp(batch))
			require.NoError(t, err)
			require.LessOrEqual(t, len(queryStr), maxTSDBQueryBytes)
			for _, d := range batch {
				seen[d]++
			}
		}
		require.Len(t, seen, len(digests))
		for _, n := range seen {
			require.Equal(t, 1, n)
		}
	})

	t.Run("cpu top 100 fits one batch", func(t *testing.T) {
		digests := make([]string, 100)
		for i := range digests {
			digests[i] = digestLabel(i)
		}
		batches, err := splitTopSQLDigests(digests, buildCPUTops)
		require.NoError(t, err)
		require.Len(t, batches, 1)
	})

	t.Run("network top 100 splits after zero-filling doubles the matcher", func(t *testing.T) {
		digests := make([]string, 100)
		for i := range digests {
			digests[i] = digestLabel(i)
		}
		// Zero-filling repeats each metric once more, so a network batch now embeds the
		// digest matcher four times; the review-measured old draft assembled tops and
		// others into one 26,455-byte query against the 16,384 limit.
		batches, err := splitTopSQLDigests(digests, buildNetworkTops)
		require.NoError(t, err)
		require.Len(t, batches, 2)

		for _, batch := range batches {
			batchQuery, err := buildTopSummaryBatchQuery(OrderByNetwork, 6, instance, instanceType, ", sql_digest=~"+sqlDigestRegexp(batch))
			require.NoError(t, err)
			require.LessOrEqual(t, len(batchQuery), maxTSDBQueryBytes)
		}

		othersQuery, err := buildTopOthersQuery(OrderByNetwork, 6, instance, instanceType, digests)
		require.NoError(t, err)
		require.Greater(t, len(othersQuery), maxTSDBQueryBytes)
	})

	t.Run("network top 200 falls back to total-minus-top for others", func(t *testing.T) {
		digests := make([]string, 200)
		for i := range digests {
			digests[i] = digestLabel(i)
		}
		batches, err := splitTopSQLDigests(digests, buildNetworkTops)
		require.NoError(t, err)
		require.Len(t, batches, 4)

		othersQuery, err := buildTopOthersQuery(OrderByNetwork, 6, instance, instanceType, digests)
		require.NoError(t, err)
		require.Greater(t, len(othersQuery), maxTSDBQueryBytes)
	})

	t.Run("cpu top 200 needs no split once others is a separate query", func(t *testing.T) {
		// The review-measured 26,229 bytes was tops and others assembled into one query;
		// separated, each side is ~13.1 KB and fits the budget without batching.
		digests := make([]string, 200)
		for i := range digests {
			digests[i] = digestLabel(i)
		}
		batches, err := splitTopSQLDigests(digests, buildCPUTops)
		require.NoError(t, err)
		require.Len(t, batches, 1)

		othersQuery, err := buildTopOthersQuery(OrderByCPU, 6, instance, instanceType, digests)
		require.NoError(t, err)
		require.LessOrEqual(t, len(othersQuery), maxTSDBQueryBytes)
	})

	t.Run("cpu top 300 splits into two batches", func(t *testing.T) {
		digests := make([]string, 300)
		for i := range digests {
			digests[i] = digestLabel(i)
		}
		batches, err := splitTopSQLDigests(digests, buildCPUTops)
		require.NoError(t, err)
		require.Len(t, batches, 2)
	})
}

func TestBuildTopSQLDigestsQuery(t *testing.T) {
	selector := `instance="127.0.0.1:20160", instance_type="tikv", sql_digest!=""`

	// Single metric: one contiguous window.
	queryStr, err := buildTopSQLDigestsQuery(OrderByCPU, 906, 5, "tikv", selector)
	require.NoError(t, err)
	require.Equal(t, "topk(5, sum(sum_over_time(cpu_time{"+selector+"}[906])) by (sql_digest))", queryStr)

	// Composite metric: both sides zero-filled, so the long-window ranking equals the sum
	// of the timeline's per-step values even for one-sided samples.
	queryStr, err = buildTopSQLDigestsQuery(OrderByNetwork, 906, 5, "tikv", selector)
	require.NoError(t, err)
	zeroFill := func(metric, other string) string {
		e := "sum_over_time(" + metric + "{" + selector + "}[906])"
		o := "sum_over_time(" + other + "{" + selector + "}[906])"
		return "(" + e + " or (" + o + " * 0))"
	}
	require.Equal(t,
		"topk(5, sum("+zeroFill("network_in_bytes", "network_out_bytes")+" + "+zeroFill("network_out_bytes", "network_in_bytes")+") by (sql_digest))",
		queryStr)
}

func TestSubtractTopFromTotal(t *testing.T) {
	mk := func(pairs ...float64) []recordsMetricRespDataResultValue {
		values := make([]recordsMetricRespDataResultValue, 0, len(pairs)/2)
		for i := 0; i < len(pairs); i += 2 {
			values = append(values, recordsMetricRespDataResultValue{pairs[i], strconv.FormatFloat(pairs[i+1], 'f', -1, 64)})
		}
		return values
	}

	total := recordsMetricRespDataResult{Values: mk(100, 10, 106, 7)}
	top := []recordsMetricRespDataResult{
		{Values: mk(100, 4, 106, 4, 112, 9)},
		{Values: mk(100, 1)},
	}
	values, err := subtractTopFromTotal(total, top)
	require.NoError(t, err)
	require.Equal(t, []recordsMetricRespDataResultValue{
		{float64(100), "5"},
		{float64(106), "3"},
	}, values)
}

func TestRejectOversizedSingleDigestQuery(t *testing.T) {
	_, err := splitTopSQLDigests([]string{"aa"}, func(matcher string) (string, error) {
		return buildTopSummaryBatchQuery(OrderByCPU, 6, strings.Repeat("x", maxTSDBQueryBytes), "tikv", matcher)
	})
	require.ErrorContains(t, err, "even with one digest")
}

func TestSplitTopSQLDigestsLimitsExpressionConstruction(t *testing.T) {
	// Constructing and escaping every growing prefix makes large Top selections
	// allocate quadratically. Keep the number of full expression builds bounded
	// while preserving the partition and byte-budget contract.
	digests := make([]string, 500)
	for i := range digests {
		digests[i] = digestLabel(i)
	}
	builds := 0
	batches, err := splitTopSQLDigests(digests, func(matcher string) (string, error) {
		builds++
		return buildTopSummaryBatchQuery(OrderByCPU, 6, "instance", "tikv", matcher)
	})
	require.NoError(t, err)
	var joined []string
	for _, batch := range batches {
		joined = append(joined, batch...)
	}
	require.Equal(t, digests, joined)
	require.LessOrEqual(t, builds, 64, "avoid rebuilding the complete expression for every digest prefix")
	t.Logf("digests=%d batches=%d expression_builds=%d", len(digests), len(batches), builds)
}

func TestSubtractSelectedTotalZero(t *testing.T) {
	other, err := subtractSelectedTotal(10, 10)
	require.NoError(t, err)
	require.Zero(t, other)
}
