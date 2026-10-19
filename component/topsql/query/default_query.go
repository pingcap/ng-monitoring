package query

import (
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/ng-monitoring/component/topsql/codec/plan"
	"github.com/pingcap/ng-monitoring/component/topsql/store"
	"github.com/pingcap/ng-monitoring/database/docdb"
	"github.com/pingcap/ng-monitoring/utils"

	"github.com/pingcap/log"
	"github.com/wangjohn/quickselect"
	"go.uber.org/zap"
)

var (
	bytesP  = utils.BytesBufferPool{}
	headerP = utils.HeaderPool{}

	sqlGroupSliceP = sqlGroupSlicePool{}
	sqlDigestMapP  = sqlDigestMapPool{}
	sumMapP        = sumMapPool{}
)

const (
	// AggLevelQuery is the query level aggregation
	AggLevelQuery = "query"
	// AggLevelTable is the table level aggregation
	AggLevelTable = "table"
	// AggLevelDB is the db level aggregation
	AggLevelDB = "db"
	// AggLevelRegion is the region level aggregation (request parameter value)
	AggLevelRegion = "region"

	// AggByRegionID is the label key for region aggregation in timeseries DB.
	AggByRegionID = "region_id"

	// maxTSDBQueryBytes keeps every assembled TSDB query below VictoriaMetrics'
	// -search.maxQueryLen (default 16 KiB); longer queries are rejected with a 422,
	// which the summary handler reports as a 503. Composite metrics repeat the digest
	// matcher several times, so the fully
	// assembled query, not the matcher alone, is what must stay within the budget.
	maxTSDBQueryBytes = 15 * 1024
)

type DefaultQuery struct {
	vmselectHandler http.HandlerFunc
	docDB           docdb.DocDB
}

func NewDefaultQuery(vmselectHandler http.HandlerFunc, docDB docdb.DocDB) *DefaultQuery {
	return &DefaultQuery{
		vmselectHandler: vmselectHandler,
		docDB:           docDB,
	}
}

var _ Query = &DefaultQuery{}

func (dq *DefaultQuery) Records(name string, startSecs, endSecs, windowSecs, top int, instance, instanceType string, fill *[]RecordItem) error {
	if startSecs > endSecs {
		return nil
	}

	// adjust start to make result align to end
	startSecs = endSecs - (endSecs-startSecs)/windowSecs*windowSecs

	var recordsResponse recordsMetricResp
	if err := dq.fetchRecordsFromTSDB(name, startSecs, endSecs, windowSecs, instance, instanceType, &recordsResponse); err != nil {
		return err
	}

	if len(recordsResponse.Data.Results) == 0 {
		return nil
	}

	sqlGroups := sqlGroupSliceP.Get()
	defer sqlGroupSliceP.Put(sqlGroups)
	topK(recordsResponse.Data.Results, top, sqlGroups)

	return dq.fillText(context.Background(), name, sqlGroups, func(item RecordItem) {
		*fill = append(*fill, item)
	})
}

func (dq *DefaultQuery) SummaryBy(startSecs, endSecs, windowSecs, top int, instance, instanceType, aggBy string, fill *[]SummaryByItem, orderBy string) error {
	if startSecs > endSecs {
		return nil
	}

	// adjust start to make result align to end
	alignStartSecs := endSecs - (endSecs-startSecs)/windowSecs*windowSecs

	orderBy = NormalizeOrderBy(orderBy)
	queryStr, err := buildOrderBySummaryByTopQuery(orderBy, windowSecs, instance, instanceType, aggBy, top)
	if err != nil {
		return err
	}

	var recordsResponse recordsMetricRespV2
	if err := dq.fetchQueryRange(context.Background(), queryStr, alignStartSecs, endSecs, windowSecs, &recordsResponse); err != nil {
		return err
	}
	if len(recordsResponse.Data.Results) == 0 {
		return nil
	}

	appendMetric := func(item *SummaryByItem, v uint64) {
		switch orderBy {
		case "", OrderByCPU:
			item.CPUTimeMs = append(item.CPUTimeMs, v)
		case OrderByNetwork:
			item.NetworkBytes = append(item.NetworkBytes, v)
		case OrderByLogicalIO:
			item.LogicalIoBytes = append(item.LogicalIoBytes, v)
		case OrderByLogicalRead:
			item.LogicalReadBytes = append(item.LogicalReadBytes, v)
		case OrderByLogicalWrite:
			item.LogicalWriteBytes = append(item.LogicalWriteBytes, v)
		case OrderByBlockRead:
			item.RocksdbBlockReadCount = append(item.RocksdbBlockReadCount, v)
		}
	}
	setSum := func(item *SummaryByItem, sum uint64) {
		switch orderBy {
		case "", OrderByCPU:
			item.CPUTimeMsSum = sum
		case OrderByNetwork:
			item.NetworkBytesSum = sum
		case OrderByLogicalIO:
			item.LogicalIoBytesSum = sum
		case OrderByLogicalRead:
			item.LogicalReadBytesSum = sum
		case OrderByLogicalWrite:
			item.LogicalWriteBytesSum = sum
		case OrderByBlockRead:
			item.RocksdbBlockReadCountSum = sum
		}
	}

	for _, result := range recordsResponse.Data.Results {
		text, _ := result.Metric[aggBy].(string)
		sumItem := SummaryByItem{
			Text:    text,
			IsOther: text == "other",
		}

		var valueSum uint64
		for _, value := range result.Values {
			if len(value) != 2 {
				continue
			}

			tsF, ok := value[0].(float64)
			if !ok {
				continue
			}
			rawV, ok := value[1].(string)
			if !ok {
				continue
			}
			v, err := strconv.ParseUint(rawV, 10, 64)
			if err != nil {
				continue
			}

			valueSum += v
			sumItem.TimestampSec = append(sumItem.TimestampSec, uint64(tsF))
			appendMetric(&sumItem, v)
		}

		setSum(&sumItem, valueSum)
		*fill = append(*fill, sumItem)
	}
	return nil
}

func (dq *DefaultQuery) Summary(ctx context.Context, startSecs, endSecs, windowSecs, top int, instance, instanceType string, fill *[]SummaryItem, orderBy string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if windowSecs <= 0 {
		return fmt.Errorf("window must be greater than 0")
	}
	if startSecs > endSecs {
		return nil
	}

	// adjust start to make result align to end
	alignStartSecs := endSecs - (endSecs-startSecs)/windowSecs*windowSecs

	orderBy = NormalizeOrderBy(orderBy)

	fillName := store.MetricNameCPUTime
	switch orderBy {
	case "", OrderByCPU:
		// already set
	case OrderByNetwork:
		fillName = metricNameNetworkBytes
	case OrderByLogicalIO:
		fillName = metricNameLogicalIoBytes
	case OrderByLogicalRead:
		fillName = store.MetricNameLogicalReadBytes
	case OrderByLogicalWrite:
		fillName = store.MetricNameLogicalWriteBytes
	case OrderByBlockRead:
		fillName = store.MetricNameRocksdbBlockReadCount
	}

	sqlGroups := sqlGroupSliceP.Get()
	defer sqlGroupSliceP.Put(sqlGroups)

	var topDigests []string
	if top <= 0 {
		// No limit: every digest is returned anyway, so fetch all series and group them here.
		queryStr, err := buildOrderBySummaryQuery(orderBy, windowSecs, instance, instanceType)
		if err != nil {
			return err
		}
		var recordsResponse recordsMetricResp
		if err := dq.fetchQueryRange(ctx, queryStr, alignStartSecs, endSecs, windowSecs, &recordsResponse); err != nil {
			return err
		}
		if len(recordsResponse.Data.Results) == 0 {
			return nil
		}
		topK(recordsResponse.Data.Results, top, sqlGroups)
	} else {
		// Rank digests inside the TSDB, then fetch series only for the top digests plus one
		// aggregated "others" series. Returning every digest's series to rank them here is
		// what makes this endpoint slow on instances with many digests.
		var err error
		topDigests, err = dq.fetchTopSQLDigests(ctx, orderBy, alignStartSecs, endSecs, windowSecs, top, instance, instanceType)
		if err != nil {
			return err
		}
		results, err := dq.fetchTopSummarySeries(ctx, orderBy, alignStartSecs, endSecs, windowSecs, instance, instanceType, topDigests)
		if err != nil {
			return err
		}
		if len(results) == 0 {
			return nil
		}
		// The "others" series has no sql_digest label, so it comes back as the others group.
		others := groupBySQLDigest(results, sqlGroups)
		if len(others.planSeries) > 0 {
			*sqlGroups = append(*sqlGroups, others)
		}
	}

	if err := dq.fillText(ctx, fillName, sqlGroups, func(item RecordItem) {
		sumItem := SummaryItem{
			SQLDigest: item.SQLDigest,
			SQLText:   item.SQLText,
			IsOther:   item.IsOther,
		}

		for _, p := range item.Plans {
			plan := SummaryPlanItem{
				PlanDigest:   p.PlanDigest,
				PlanText:     p.PlanText,
				TimestampSec: p.TimestampSec,
			}
			switch orderBy {
			case "", OrderByCPU:
				plan.CPUTimeMs = p.CPUTimeMs
				for _, v := range p.CPUTimeMs {
					sumItem.CPUTimeMs += v
				}
			case OrderByNetwork:
				plan.NetworkBytes = p.NetworkBytes
				for _, v := range p.NetworkBytes {
					sumItem.NetworkBytes += v
				}
			case OrderByLogicalIO:
				plan.LogicalIoBytes = p.LogicalIoBytes
				for _, v := range p.LogicalIoBytes {
					sumItem.LogicalIoBytes += v
				}
			case OrderByLogicalRead:
				plan.LogicalReadBytes = p.LogicalReadBytes
				for _, v := range p.LogicalReadBytes {
					sumItem.LogicalReadBytes += v
				}
			case OrderByLogicalWrite:
				plan.LogicalWriteBytes = p.LogicalWriteBytes
				for _, v := range p.LogicalWriteBytes {
					sumItem.LogicalWriteBytes += v
				}
			case OrderByBlockRead:
				plan.RocksdbBlockReadCount = p.RocksdbBlockReadCount
				for _, v := range p.RocksdbBlockReadCount {
					sumItem.RocksdbBlockReadCount += v
				}
			}
			sumItem.Plans = append(sumItem.Plans, plan)
		}

		*fill = append(*fill, sumItem)
	}); err != nil {
		return err
	}

	return dq.fillSummaryExtraFields(ctx, startSecs, endSecs, instance, instanceType, topDigests, top > 0, fill)
}

func (dq *DefaultQuery) Instances(startSecs, endSecs int, fill *[]InstanceItem) error {
	if startSecs > endSecs {
		return nil
	}
	return dq.fetchInstancesFromTSDB(startSecs, endSecs, fill)
}

func (dq *DefaultQuery) Close() {}

type planSeries struct {
	planDigest    string
	timestampSecs []uint64
	values        []uint64
}

type sqlGroup struct {
	sqlDigest  string
	planSeries []planSeries
	valueSum   uint64
}

func (dq *DefaultQuery) fetchRecordsFromTSDB(name string, startSecs int, endSecs int, windowSecs int, instance, instanceType string, metricResponse *recordsMetricResp) error {
	queryStr := fmt.Sprintf("sum_over_time(%s{instance=\"%s\", instance_type=\"%s\"}[%d])", name, instance, instanceType, windowSecs)
	return dq.fetchQueryRange(context.Background(), queryStr, startSecs, endSecs, windowSecs, metricResponse)
}

// fetchQueryRange runs a range query against the embedded TSDB. ctx is attached to the request so
// that a client that has gone away (e.g. a gateway timeout) stops the remaining queries instead of
// leaving them running to completion.
func (dq *DefaultQuery) fetchQueryRange(ctx context.Context, queryStr string, startSecs int, endSecs int, stepSecs int, metricResponse interface{}) error {
	params := url.Values{
		"query": {queryStr},
		"start": {strconv.Itoa(startSecs)},
		"end":   {strconv.Itoa(endSecs)},
		"step":  {strconv.Itoa(stepSecs)},
	}
	return dq.fetchQuery(ctx, "/api/v1/query_range", params, metricResponse)
}

// fetchQuery shares the request's remaining budget across TSDB calls. VM uses
// timeout for cooperative execution checks; context also cancels queued work.
func (dq *DefaultQuery) fetchQuery(ctx context.Context, path string, params url.Values, target interface{}) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if dq.vmselectHandler == nil {
		return fmt.Errorf("empty query handler")
	}
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return context.DeadlineExceeded
		}
		// VM interprets a zero timeout as its default, so keep at least 1ms.
		params.Set("timeout", strconv.FormatInt(max(1, remaining.Milliseconds()), 10)+"ms")
	}
	params.Set("nocache", "1")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, path, nil)
	if err != nil {
		return err
	}
	req.URL.RawQuery = params.Encode()
	req.Header.Set("Accept", "application/json")

	bufResp := bytesP.Get()
	header := headerP.Get()
	defer bytesP.Put(bufResp)
	defer headerP.Put(header)
	resp := utils.NewRespWriter(bufResp, header)
	dq.vmselectHandler(&resp, req)
	// An executing VM query need not observe cancellation immediately. Avoid
	// decoding its response or starting a later stage after it returns.
	if err := ctx.Err(); err != nil {
		return err
	}
	if resp.Code < 200 || resp.Code >= 300 {
		errStr := resp.Body.String()
		log.Warn("failed to fetch timeseries db", zap.String("error", errStr))
		return errors.New(errStr)
	}
	if err := json.Unmarshal(resp.Body.Bytes(), target); err != nil {
		return err
	}
	return ctx.Err()
}

func (dq *DefaultQuery) fetchInstancesFromTSDB(startSecs, endSecs int, fill *[]InstanceItem) error {
	if dq.vmselectHandler == nil {
		return fmt.Errorf("empty query handler")
	}

	bufResp := bytesP.Get()
	header := headerP.Get()

	defer bytesP.Put(bufResp)
	defer headerP.Put(header)
	req, err := http.NewRequest("GET", "/api/v1/query", nil)
	if err != nil {
		return err
	}
	reqQuery := req.URL.Query()

	// Data:
	// t1  t2  t3  t4  t5  t6  t7  t8  t9
	// v1  v2  v3  v4  v5  v6  v7  v8  v9
	//
	// Given startSecs = t3, endSecs = t6, to calculate v3 || v4 || v5 || v6
	//
	// The exec model for vm is lookbehind. So we can set time = endSecs, window = endSecs-startSecs+1,
	// to get the point that exists from v3 to v6.
	//
	// t1  t2  t3  t4  t5  t6  t7  t8  t9
	// v1  v2  v3  v4  v5  v6  v7  v8  v9
	//         ^            ^
	//         |   window   |
	reqQuery.Set("query", fmt.Sprintf("last_over_time(instance[%ds])", endSecs-startSecs+1))
	reqQuery.Set("time", strconv.Itoa(endSecs))
	reqQuery.Set("nocache", "1")
	req.URL.RawQuery = reqQuery.Encode()
	req.Header.Set("Accept", "application/json")
	respR := utils.NewRespWriter(bufResp, header)
	dq.vmselectHandler(&respR, req)
	if statusOK := respR.Code >= 200 && respR.Code < 300; !statusOK {
		errStr := respR.Body.String()
		log.Warn("failed to fetch timeseries db", zap.String("error", errStr))
		return errors.New(errStr)
	}

	res := instancesMetricResp{}
	if err = json.Unmarshal(respR.Body.Bytes(), &res); err != nil {
		return err
	}
	for _, result := range res.Data.Results {
		*fill = append(*fill, InstanceItem{
			Instance:     result.Metric.Instance,
			InstanceType: result.Metric.InstanceType,
		})
	}

	return nil
}

// fetchInstantQuery runs an instant query against the embedded TSDB at timeSecs. stepSecs
// is passed to the TSDB explicitly: without it the TSDB defaults to a 5m step, which widens
// the underlying search range to max(window, step) and makes short ranges scan samples far
// outside the queried window.
func (dq *DefaultQuery) fetchInstantQuery(ctx context.Context, queryStr string, timeSecs, stepSecs int, metricResponse interface{}) error {
	params := url.Values{
		"query": {queryStr},
		"time":  {strconv.Itoa(timeSecs)},
	}
	if stepSecs > 0 {
		params.Set("step", strconv.Itoa(stepSecs))
	}
	return dq.fetchQuery(ctx, "/api/v1/query", params, metricResponse)
}

// fetchTopSQLDigests returns the top SQL digests ranked by the orderBy metric summed over
// the points a range query on [alignStartSecs, endSecs] with step windowSecs would cover.
// The ranking runs inside the TSDB so that only `top` series are returned.
// The selection is fixed for this request. Ranking and later queries do not share
// a read snapshot: late samples can change curves without re-ranking the selection.
func (dq *DefaultQuery) fetchTopSQLDigests(ctx context.Context, orderBy string, alignStartSecs, endSecs, windowSecs, top int, instance, instanceType string) ([]string, error) {
	// Each range point at t sums (t-windowSecs, t], so all points together cover
	// (alignStartSecs-windowSecs, endSecs].
	rangeSecs := endSecs - alignStartSecs + windowSecs
	selector := summarySelector(instance, instanceType, `, sql_digest!=""`)
	queryStr, err := buildTopSQLDigestsQuery(orderBy, rangeSecs, top, instanceType, selector)
	if err != nil {
		return nil, err
	}

	var resp sumMetricResp
	if err := dq.fetchInstantQuery(ctx, queryStr, endSecs, windowSecs, &resp); err != nil {
		return nil, err
	}
	digests := make([]string, 0, len(resp.Data.Results))
	for _, r := range resp.Data.Results {
		if r.Metric.SQLDigest != "" {
			digests = append(digests, r.Metric.SQLDigest)
		}
	}
	return digests, nil
}

// buildTopSQLDigestsQuery returns the instant query ranking digests by the orderBy metric
// summed over one window ending at endSecs. Composite metrics zero-fill their counterpart
// side (see compositeSum), so the ranking total equals the sum of the timeline's per-step
// values and cannot select a digest the timeline would not show.
func buildTopSQLDigestsQuery(orderBy string, rangeSecs, top int, instanceType, selector string) (string, error) {
	expr, err := buildSummaryExpr(orderBy, rangeSecs, instanceType, selector)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("topk(%d, sum(%s) by (sql_digest))", top, expr), nil
}

// fetchSumFromTSDB sums the metric over [startSecs, endSecs] per (sql digest, plan digest).
// topLimited reports whether the request bounds the returned SQL set (top > 0): with a
// limit, only the selected digests are returned individually and every other digest folds
// into a single entry keyed by empty digests, which callers treat as others. An unlimited
// request returns every digest individually.
func (dq *DefaultQuery) fetchSumFromTSDB(ctx context.Context, name string, startSecs, endSecs int, instance, instanceType string, topDigests []string, topLimited bool, fill map[RecordKey]float64) error {
	// Data:
	// t1  t2  t3  t4  t5  t6  t7  t8  t9
	// v1  v2  v3  v4  v5  v6  v7  v8  v9
	//
	// Given startSecs = t3, endSecs = t6, to calculate v3 + v4 + v5 + v6
	//
	// The exec model for vm is lookbehind. So we can set time = endSecs, window = endSecs-startSecs+1,
	// to get the point that sum up from v3 to v6.
	//
	// t1  t2  t3  t4  t5  t6  t7  t8  t9
	// v1  v2  v3  v4  v5  v6  v7  v8  v9
	//         ^            ^
	//         |   window   |
	//
	// Aggregate by (sql_digest, plan_digest): TiKV series also carry db/table labels, so the
	// same digest pair can appear once per table.
	windowSecs := endSecs - startSecs + 1
	sumExpr := func(extraMatchers string) string {
		return fmt.Sprintf("sum(sum_over_time(%s{%s}[%ds])) by (sql_digest, plan_digest)",
			name, summarySelector(instance, instanceType, extraMatchers), windowSecs)
	}
	totalExpr := func(extraMatchers string) string {
		return fmt.Sprintf("sum(sum_over_time(%s{%s}[%ds]))",
			name, summarySelector(instance, instanceType, extraMatchers), windowSecs)
	}

	if len(topDigests) == 0 {
		if !topLimited {
			// Unlimited request: every digest is returned individually.
			var sumResponse sumMetricResp
			if err := dq.fetchInstantQuery(ctx, sumExpr(""), endSecs, windowSecs, &sumResponse); err != nil {
				return err
			}
			collectSumResults(sumResponse, fill)
			return nil
		}
		// Limited request with an empty selection: only the global "others" aggregate is
		// needed, matching the aggregated timeline, instead of every digest individually.
		var sumResponse sumMetricResp
		if err := dq.fetchInstantQuery(ctx, totalExpr(""), endSecs, windowSecs, &sumResponse); err != nil {
			return err
		}
		collectSumResults(sumResponse, fill)
		return nil
	}

	// Split the selected digests so every assembled query stays within maxTSDBQueryBytes;
	// budget applies to the complete expression, including every matcher occurrence.
	batches, err := splitTopSQLDigests(topDigests, func(matcher string) (string, error) {
		return sumExpr(matcher), nil
	})
	if err != nil {
		return err
	}
	topTotal := 0.0
	for _, batch := range batches {
		queryStr := sumExpr(", sql_digest=~" + sqlDigestRegexp(batch))
		var sumResponse sumMetricResp
		if err := dq.fetchInstantQuery(ctx, queryStr, endSecs, windowSecs, &sumResponse); err != nil {
			return err
		}
		topTotal += collectSumResults(sumResponse, fill)
	}

	// The others entry excludes every selected digest. When the fully assembled negative
	// query fits the budget, run it as is; otherwise derive others from the unfiltered
	// total minus the selected sums, so the query length does not grow with the selection.
	othersQuery := totalExpr(", sql_digest!~" + sqlDigestRegexp(topDigests))
	if len(othersQuery) <= maxTSDBQueryBytes {
		var sumResponse sumMetricResp
		if err := dq.fetchInstantQuery(ctx, othersQuery, endSecs, windowSecs, &sumResponse); err != nil {
			return err
		}
		collectSumResults(sumResponse, fill)
		return nil
	}
	var sumResponse sumMetricResp
	if err := dq.fetchInstantQuery(ctx, totalExpr(""), endSecs, windowSecs, &sumResponse); err != nil {
		return err
	}
	if len(sumResponse.Data.Results) == 0 {
		return nil
	}
	total, ok := parseSumValue(sumResponse.Data.Results[0].Value)
	if !ok {
		return nil
	}
	key := RecordKey{}
	if _, exists := fill[key]; !exists {
		fill[key] = total - topTotal
	}
	return nil
}

// parseSumValue extracts the numeric value of an instant-query result point.
func parseSumValue(value []interface{}) (float64, bool) {
	if len(value) < 2 {
		return 0, false
	}
	v, ok := value[1].(string)
	if !ok {
		return 0, false
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, false
	}
	return f, true
}

// collectSumResults stores instant-query results into fill, keyed by (sql, plan) digests;
// an unlabeled result lands on the empty key, which callers treat as others. It returns the
// sum of all stored values so callers can derive others by subtraction.
func collectSumResults(resp sumMetricResp, fill map[RecordKey]float64) float64 {
	total := 0.0
	for _, result := range resp.Data.Results {
		v, ok := parseSumValue(result.Value)
		if !ok {
			continue
		}
		key := RecordKey{
			SQLDigest:  result.Metric.SQLDigest,
			PlanDigest: result.Metric.PlanDigest,
		}
		fill[key] = v
		total += v
	}
	return total
}

func topK(results []recordsMetricRespDataResult, top int, sqlGroups *[]sqlGroup) {
	originalOthers := groupBySQLDigest(results, sqlGroups)
	queryOthers := keepTopK(sqlGroups, top)
	others := mergeOthers(originalOthers, queryOthers)

	if len(others.planSeries) > 0 {
		*sqlGroups = append(*sqlGroups, others)
	}
}

// convert data to a model as list[(SQL digest, list[(plan digest, (list[timestamp], list[value]))])]
//
// In addition, empty SQL digest is a special SQL digest. It means 'other' data points which is evicted during
// collection. Here, these data points will be distinguished out and returned.
func groupBySQLDigest(resp []recordsMetricRespDataResult, target *[]sqlGroup) (others sqlGroup) {
	m := sqlDigestMapP.Get()
	defer sqlDigestMapP.Put(m)

	for _, r := range resp {
		groupKey := r.Metric.SQLDigest
		group := m[groupKey]
		group.sqlDigest = r.Metric.SQLDigest

		var ps *planSeries

		plan := r.Metric.PlanDigest
		for i, s := range group.planSeries {
			if s.planDigest == plan {
				ps = &group.planSeries[i]
				break
			}
		}
		if ps == nil {
			group.planSeries = append(group.planSeries, planSeries{
				planDigest: plan,
			})
			ps = &group.planSeries[len(group.planSeries)-1]
		}

		for _, value := range r.Values {
			if len(value) != 2 {
				continue
			}

			tsF, ok := value[0].(float64)
			if !ok {
				continue
			}

			rawV, ok := value[1].(string)
			if !ok {
				continue
			}

			ts := uint64(tsF)
			v, err := strconv.ParseUint(rawV, 10, 64)
			if err != nil {
				continue
			}

			group.valueSum += v
			ps.timestampSecs = append(ps.timestampSecs, ts)
			ps.values = append(ps.values, v)
		}

		m[groupKey] = group
	}

	for _, group := range m {
		if group.sqlDigest == "" {
			others = group
			continue
		}

		*target = append(*target, sqlGroup{
			sqlDigest:  group.sqlDigest,
			planSeries: group.planSeries,
			valueSum:   group.valueSum,
		})
	}

	return
}

func keepTopK(groups *[]sqlGroup, top int) (others []sqlGroup) {
	if top <= 0 || len(*groups) <= top {
		return nil
	}

	// k is in the range [0, data.Len()), never return error
	_ = quickselect.QuickSelect(TopKSlice(*groups), top)

	*groups, others = (*groups)[:top], (*groups)[top:]
	return
}

func mergeOthers(originalOthers sqlGroup, queryOthers []sqlGroup) (res sqlGroup) {
	if len(queryOthers) == 0 {
		return originalOthers
	}

	h := &tsHeap{}
	heap.Init(h)
	for _, p := range originalOthers.planSeries {
		for i := range p.timestampSecs {
			heap.Push(h, tsItem{
				timestampSecs: p.timestampSecs[i],
				v:             p.values[i],
			})
		}
	}
	for _, q := range queryOthers {
		for _, p := range q.planSeries {
			for i := range p.timestampSecs {
				heap.Push(h, tsItem{
					timestampSecs: p.timestampSecs[i],
					v:             p.values[i],
				})
			}
		}
	}

	res.planSeries = append(res.planSeries, planSeries{})
	ps := &res.planSeries[0]
	if h.Len() == 0 {
		return
	}

	first := heap.Pop(h).(tsItem)
	ps.timestampSecs = append(ps.timestampSecs, first.timestampSecs)
	ps.values = append(ps.values, first.v)
	for h.Len() > 0 {
		x := heap.Pop(h).(tsItem)
		lastTs := ps.timestampSecs[len(ps.timestampSecs)-1]
		if x.timestampSecs == lastTs {
			ps.values[len(ps.timestampSecs)-1] += x.v
		} else {
			ps.timestampSecs = append(ps.timestampSecs, x.timestampSecs)
			ps.values = append(ps.values, x.v)
		}
	}

	return
}

func (dq *DefaultQuery) fillText(ctx context.Context, name string, sqlGroups *[]sqlGroup, fill func(RecordItem)) error {
	var err error
	for _, group := range *sqlGroups {
		if err := ctx.Err(); err != nil {
			return err
		}
		sqlDigest := group.sqlDigest
		var sqlText string

		if len(sqlDigest) != 0 {
			sqlText, err = dq.docDB.QuerySQLMeta(ctx, sqlDigest)
			if err != nil {
				return err
			}
		}

		item := RecordItem{
			SQLDigest: sqlDigest,
			SQLText:   sqlText,
			IsOther:   len(sqlDigest) == 0,
		}

		for _, series := range group.planSeries {
			if err := ctx.Err(); err != nil {
				return err
			}
			planDigest := series.planDigest
			var planText string
			var encodedPlan string

			if len(planDigest) != 0 {
				planText, encodedPlan, err = dq.docDB.QueryPlanMeta(ctx, planDigest)
				if err != nil {
					return err
				}
			}

			if err := ctx.Err(); err != nil {
				return err
			}

			planItem := RecordPlanItem{
				PlanDigest:   planDigest,
				TimestampSec: series.timestampSecs,
			}

			if len(planText) > 0 {
				planItem.PlanText = planText
			} else if len(encodedPlan) > 0 {
				var err error
				planItem.PlanText, err = plan.Decode(encodedPlan)
				if err != nil {
					log.Warn("failed to decode plan", zap.Error(err), zap.String("encoded plan", encodedPlan))
				}
			}

			switch name {
			case store.MetricNameCPUTime:
				planItem.CPUTimeMs = series.values
			case metricNameNetworkBytes:
				planItem.NetworkBytes = series.values
			case metricNameLogicalIoBytes:
				planItem.LogicalIoBytes = series.values
			case store.MetricNameLogicalReadBytes:
				planItem.LogicalReadBytes = series.values
			case store.MetricNameLogicalWriteBytes:
				planItem.LogicalWriteBytes = series.values
			case store.MetricNameRocksdbBlockReadCount:
				planItem.RocksdbBlockReadCount = series.values
			case store.MetricNameReadRow:
				planItem.ReadRows = series.values
			case store.MetricNameReadIndex:
				planItem.ReadIndexes = series.values
			case store.MetricNameWriteRow:
				planItem.WriteRows = series.values
			case store.MetricNameWriteIndex:
				planItem.WriteIndexes = series.values
			case store.MetricNameSQLExecCount:
				planItem.SQLExecCount = series.values
			case store.MetricNameSQLDurationSum:
				planItem.SQLDurationSum = series.values
			case store.MetricNameSQLDurationCount:
				planItem.SQLDurationCount = series.values
			}
			item.Plans = append(item.Plans, planItem)
		}

		fill(item)
	}

	return ctx.Err()
}

type TopKSlice []sqlGroup

var _ sort.Interface = TopKSlice{}

func (s TopKSlice) Len() int {
	return len(s)
}

func (s TopKSlice) Less(i, j int) bool {
	si := s[i]
	sj := s[j]

	if si.valueSum != sj.valueSum {
		return si.valueSum > sj.valueSum
	}

	return si.sqlDigest > sj.sqlDigest
}

func (s TopKSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

type tsItem struct {
	timestampSecs uint64
	v             uint64
}
type tsHeap []tsItem

var _ heap.Interface = &tsHeap{}

func (t tsHeap) Len() int           { return len(t) }
func (t tsHeap) Less(i, j int) bool { return t[i].timestampSecs < t[j].timestampSecs }
func (t tsHeap) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }

func (t *tsHeap) Push(x interface{}) {
	*t = append(*t, x.(tsItem))
}

func (t *tsHeap) Pop() interface{} {
	old := *t
	n := len(old)
	x := old[n-1]
	*t = old[0 : n-1]
	return x
}

func buildOrderBySummaryQuery(orderBy string, windowSecs int, instance, instanceType string) (string, error) {
	return buildSummaryExpr(orderBy, windowSecs, instanceType, summarySelector(instance, instanceType, ""))
}

// summarySelector returns the label matchers selecting one instance, followed by extraMatchers
// (which must start with ", " when not empty).
func summarySelector(instance, instanceType, extraMatchers string) string {
	return fmt.Sprintf("instance=%q, instance_type=%q%s", instance, instanceType, extraMatchers)
}

// sqlDigestRegexp returns a quoted regexp matching exactly the given SQL digests.
func sqlDigestRegexp(digests []string) string {
	quoted := make([]string, 0, len(digests))
	for _, d := range digests {
		quoted = append(quoted, regexp.QuoteMeta(d))
	}
	return strconv.Quote(strings.Join(quoted, "|"))
}

// buildSummaryExpr returns the per-series expression of the orderBy metric summed over windowSecs.
func buildSummaryExpr(orderBy string, windowSecs int, instanceType, selector string) (string, error) {
	switch NormalizeOrderBy(orderBy) {
	case "", OrderByCPU:
		return fmt.Sprintf("sum_over_time(%s{%s}[%d])", store.MetricNameCPUTime, selector, windowSecs), nil
	case OrderByNetwork:
		in, out := store.MetricNameNetworkInBytes, store.MetricNameNetworkOutBytes
		if instanceType == "tidb" {
			in, out = store.MetricNameSQLNetworkIn, store.MetricNameSQLNetworkOut
		}
		return compositeSum(in, out, selector, windowSecs), nil
	case OrderByLogicalIO:
		return compositeSum(store.MetricNameLogicalReadBytes, store.MetricNameLogicalWriteBytes, selector, windowSecs), nil
	case OrderByLogicalRead:
		return fmt.Sprintf("sum_over_time(%s{%s}[%d])", store.MetricNameLogicalReadBytes, selector, windowSecs), nil
	case OrderByLogicalWrite:
		return fmt.Sprintf("sum_over_time(%s{%s}[%d])", store.MetricNameLogicalWriteBytes, selector, windowSecs), nil
	case OrderByBlockRead:
		return fmt.Sprintf("sum_over_time(%s{%s}[%d])", store.MetricNameRocksdbBlockReadCount, selector, windowSecs), nil
	default:
		return "", fmt.Errorf("unknown orderBy: %s", orderBy)
	}
}

// compositeSum combines two metrics of the same series with their counterpart side
// zero-filled: `(in or out*0) + (out or in*0)`. A plain binary op drops a series whose
// counterpart metric is missing in the current window, which makes the per-step timeline
// values and the long-window ranking disagree for one-sided samples; zero-filling keeps the
// long-window total equal to the sum of the timeline's per-step values, and counts one-sided
// bytes instead of silently dropping them.
func compositeSum(in, out, selector string, windowSecs int) string {
	inExpr := fmt.Sprintf("sum_over_time(%s{%s}[%d])", in, selector, windowSecs)
	outExpr := fmt.Sprintf("sum_over_time(%s{%s}[%d])", out, selector, windowSecs)
	inFilled := fmt.Sprintf("(%s or (%s * 0))", inExpr, outExpr)
	outFilled := fmt.Sprintf("(%s or (%s * 0))", outExpr, inExpr)
	return fmt.Sprintf("%s + %s", inFilled, outFilled)
}

// splitTopSQLDigests splits digests into batches whose fully assembled tops query stays
// within maxTSDBQueryBytes. buildQuery assembles the complete query from one batch's
// positive matcher, which already starts with ", ". Every returned batch is non-empty and
// the batches partition digests in order.
func splitTopSQLDigests(digests []string, buildQuery func(matcher string) (string, error)) ([][]string, error) {
	var batches [][]string
	for i := 0; i < len(digests); {
		j := i + 1
		first, err := buildQuery(", sql_digest=~" + sqlDigestRegexp(digests[i:j]))
		if err != nil {
			return nil, err
		}
		if len(first) > maxTSDBQueryBytes {
			return nil, fmt.Errorf("Top SQL query exceeds %d bytes even with one digest", maxTSDBQueryBytes)
		}
		for j < len(digests) {
			queryStr, err := buildQuery(", sql_digest=~" + sqlDigestRegexp(digests[i:j+1]))
			if err != nil {
				return nil, err
			}
			if len(queryStr) > maxTSDBQueryBytes {
				break
			}
			j++
		}
		batches = append(batches, digests[i:j])
		i = j
	}
	return batches, nil
}

// buildTopSummaryBatchQuery returns a range query aggregating one batch of selected digests
// per (sql digest, plan digest). matcher must start with ", ".
func buildTopSummaryBatchQuery(orderBy string, windowSecs int, instance, instanceType, matcher string) (string, error) {
	expr, err := buildSummaryExpr(orderBy, windowSecs, instanceType, summarySelector(instance, instanceType, matcher))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("sum(%s) by (sql_digest, plan_digest)", expr), nil
}

// buildTotalSummaryQuery returns a label-less range query summing the orderBy metric over
// every series of the instance, selected digests or not.
func buildTotalSummaryQuery(orderBy string, windowSecs int, instance, instanceType string) (string, error) {
	expr, err := buildSummaryExpr(orderBy, windowSecs, instanceType, summarySelector(instance, instanceType, ""))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("sum(%s)", expr), nil
}

// buildTopOthersQuery returns the label-less query aggregating every digest not in
// topDigests, including records without a digest.
func buildTopOthersQuery(orderBy string, windowSecs int, instance, instanceType string, topDigests []string) (string, error) {
	if len(topDigests) == 0 {
		return buildTotalSummaryQuery(orderBy, windowSecs, instance, instanceType)
	}
	expr, err := buildSummaryExpr(orderBy, windowSecs, instanceType, summarySelector(instance, instanceType, ", sql_digest!~"+sqlDigestRegexp(topDigests)))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("sum(%s)", expr), nil
}

// fetchTopSummarySeries fetches the timeline series of the selected digests plus one
// label-less "others" series. Selected digests are split into batches so every assembled
// query stays within maxTSDBQueryBytes even when the orderBy expression repeats the digest
// matcher per metric (network, logical_io). The others series excludes all selected digests
// when that query fits the budget; otherwise it is derived as the unfiltered total minus
// the fetched top series, so no query length grows with the selection size.
func (dq *DefaultQuery) fetchTopSummarySeries(ctx context.Context, orderBy string, alignStartSecs, endSecs, windowSecs int, instance, instanceType string, topDigests []string) ([]recordsMetricRespDataResult, error) {
	if len(topDigests) == 0 {
		// Nothing is selected, so the whole range is one "others" series.
		queryStr, err := buildTotalSummaryQuery(orderBy, windowSecs, instance, instanceType)
		if err != nil {
			return nil, err
		}
		var resp recordsMetricResp
		if err := dq.fetchQueryRange(ctx, queryStr, alignStartSecs, endSecs, windowSecs, &resp); err != nil {
			return nil, err
		}
		return resp.Data.Results, nil
	}

	batches, err := splitTopSQLDigests(topDigests, func(matcher string) (string, error) {
		return buildTopSummaryBatchQuery(orderBy, windowSecs, instance, instanceType, matcher)
	})
	if err != nil {
		return nil, err
	}
	var topResults []recordsMetricRespDataResult
	for _, batch := range batches {
		queryStr, err := buildTopSummaryBatchQuery(orderBy, windowSecs, instance, instanceType, ", sql_digest=~"+sqlDigestRegexp(batch))
		if err != nil {
			return nil, err
		}
		var resp recordsMetricResp
		if err := dq.fetchQueryRange(ctx, queryStr, alignStartSecs, endSecs, windowSecs, &resp); err != nil {
			return nil, err
		}
		topResults = append(topResults, resp.Data.Results...)
	}

	othersQuery, err := buildTopOthersQuery(orderBy, windowSecs, instance, instanceType, topDigests)
	if err != nil {
		return nil, err
	}
	if len(othersQuery) <= maxTSDBQueryBytes {
		var resp recordsMetricResp
		if err := dq.fetchQueryRange(ctx, othersQuery, alignStartSecs, endSecs, windowSecs, &resp); err != nil {
			return nil, err
		}
		return append(topResults, resp.Data.Results...), nil
	}

	totalQuery, err := buildTotalSummaryQuery(orderBy, windowSecs, instance, instanceType)
	if err != nil {
		return nil, err
	}
	var resp recordsMetricResp
	if err := dq.fetchQueryRange(ctx, totalQuery, alignStartSecs, endSecs, windowSecs, &resp); err != nil {
		return nil, err
	}
	if len(resp.Data.Results) == 0 {
		return topResults, nil
	}
	others := recordsMetricRespDataResult{
		Values: subtractTopFromTotal(resp.Data.Results[0], topResults),
	}
	return append(topResults, others), nil
}

// subtractTopFromTotal computes, per timestamp, the unfiltered total minus the sum of the
// top series. Both sides come from range queries on the same grid, so point indexes align;
// timestamps missing on the top side count as zero, and timestamps missing from the total
// are skipped.
func subtractTopFromTotal(totalSeries recordsMetricRespDataResult, top []recordsMetricRespDataResult) []recordsMetricRespDataResultValue {
	topSums := make(map[float64]float64)
	for _, series := range top {
		for _, point := range series.Values {
			if len(point) < 2 {
				continue
			}
			ts, ok := point[0].(float64)
			if !ok {
				continue
			}
			v, ok := parseSumValue(point)
			if !ok {
				continue
			}
			topSums[ts] += v
		}
	}
	values := make([]recordsMetricRespDataResultValue, 0, len(totalSeries.Values))
	for _, point := range totalSeries.Values {
		if len(point) < 2 {
			continue
		}
		ts, ok := point[0].(float64)
		if !ok {
			continue
		}
		total, ok := parseSumValue(point)
		if !ok {
			continue
		}
		others := total - topSums[ts]
		values = append(values, recordsMetricRespDataResultValue{point[0], strconv.FormatFloat(others, 'f', -1, 64)})
	}
	return values
}

func buildOrderBySummaryByQuery(orderBy string, windowSecs int, instance, instanceType, aggBy string) (string, error) {
	if aggBy == AggByRegionID {
		switch NormalizeOrderBy(orderBy) {
		case "", OrderByCPU:
			return fmt.Sprintf(
				"sum(sum_over_time(%s{instance=\"%s\", instance_type=\"%s\"}[%d])) by (%s)",
				store.MetricNameRegionCPUTime, instance, instanceType, windowSecs, aggBy,
			), nil
		case OrderByNetwork:
			return fmt.Sprintf(
				"sum(sum_over_time(%s{instance=\"%s\", instance_type=\"%s\"}[%d]) + sum_over_time(%s{instance=\"%s\", instance_type=\"%s\"}[%d])) by (%s)",
				store.MetricNameRegionNetworkInBytes, instance, instanceType, windowSecs,
				store.MetricNameRegionNetworkOutBytes, instance, instanceType, windowSecs,
				aggBy,
			), nil
		case OrderByLogicalIO:
			return fmt.Sprintf(
				"sum(sum_over_time(%s{instance=\"%s\", instance_type=\"%s\"}[%d]) + sum_over_time(%s{instance=\"%s\", instance_type=\"%s\"}[%d])) by (%s)",
				store.MetricNameRegionLogicalReadBytes, instance, instanceType, windowSecs,
				store.MetricNameRegionLogicalWriteBytes, instance, instanceType, windowSecs,
				aggBy,
			), nil
		case OrderByLogicalRead:
			return fmt.Sprintf("sum(sum_over_time(%s{instance=\"%s\", instance_type=\"%s\"}[%d])) by (%s)", store.MetricNameRegionLogicalReadBytes, instance, instanceType, windowSecs, aggBy), nil
		case OrderByLogicalWrite:
			return fmt.Sprintf("sum(sum_over_time(%s{instance=\"%s\", instance_type=\"%s\"}[%d])) by (%s)", store.MetricNameRegionLogicalWriteBytes, instance, instanceType, windowSecs, aggBy), nil
		case OrderByBlockRead:
			return fmt.Sprintf("sum(sum_over_time(%s{instance=\"%s\", instance_type=\"%s\"}[%d])) by (%s)", store.MetricNameRegionRocksdbBlockReadCount, instance, instanceType, windowSecs, aggBy), nil
		default:
			return "", fmt.Errorf("unknown orderBy: %s", orderBy)
		}
	}
	switch NormalizeOrderBy(orderBy) {
	case "", OrderByCPU:
		return fmt.Sprintf(
			"sum(sum_over_time(%s{instance=\"%s\", instance_type=\"%s\"}[%d])) by (%s)",
			store.MetricNameCPUTime, instance, instanceType, windowSecs, aggBy,
		), nil
	case OrderByNetwork:
		in, out := store.MetricNameNetworkInBytes, store.MetricNameNetworkOutBytes
		if instanceType == "tidb" {
			in, out = store.MetricNameSQLNetworkIn, store.MetricNameSQLNetworkOut
		}
		return fmt.Sprintf(
			"sum(sum_over_time(%s{instance=\"%s\", instance_type=\"%s\"}[%d]) + sum_over_time(%s{instance=\"%s\", instance_type=\"%s\"}[%d])) by (%s)",
			in, instance, instanceType, windowSecs,
			out, instance, instanceType, windowSecs,
			aggBy,
		), nil
	case OrderByLogicalIO:
		return fmt.Sprintf(
			"sum(sum_over_time(%s{instance=\"%s\", instance_type=\"%s\"}[%d]) + sum_over_time(%s{instance=\"%s\", instance_type=\"%s\"}[%d])) by (%s)",
			store.MetricNameLogicalReadBytes, instance, instanceType, windowSecs,
			store.MetricNameLogicalWriteBytes, instance, instanceType, windowSecs,
			aggBy,
		), nil
	case OrderByLogicalRead:
		return fmt.Sprintf("sum(sum_over_time(%s{instance=\"%s\", instance_type=\"%s\"}[%d])) by (%s)", store.MetricNameLogicalReadBytes, instance, instanceType, windowSecs, aggBy), nil
	case OrderByLogicalWrite:
		return fmt.Sprintf("sum(sum_over_time(%s{instance=\"%s\", instance_type=\"%s\"}[%d])) by (%s)", store.MetricNameLogicalWriteBytes, instance, instanceType, windowSecs, aggBy), nil
	case OrderByBlockRead:
		return fmt.Sprintf("sum(sum_over_time(%s{instance=\"%s\", instance_type=\"%s\"}[%d])) by (%s)", store.MetricNameRocksdbBlockReadCount, instance, instanceType, windowSecs, aggBy), nil
	default:
		return "", fmt.Errorf("unknown orderBy: %s", orderBy)
	}
}

func buildOrderBySummaryByTopQuery(orderBy string, windowSecs int, instance, instanceType, aggBy string, top int) (string, error) {
	if top <= 0 {
		return buildOrderBySummaryByQuery(orderBy, windowSecs, instance, instanceType, aggBy)
	}
	base, err := buildOrderBySummaryByQuery(orderBy, windowSecs, instance, instanceType, aggBy)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("topk_avg(%d, %s, \"%s=other\")", top, base, aggBy), nil
}

func (dq *DefaultQuery) fillSummaryExtraFields(ctx context.Context, startSecs, endSecs int, instance, instanceType string, topDigests []string, topLimited bool, fill *[]SummaryItem) error {
	// rangeSecs never to be 0 because startSecs <= endSecs
	rangeSecs := float64(endSecs - startSecs + 1)

	sumDurationMap := sumMapP.Get()
	defer sumMapP.Put(sumDurationMap)
	if err := dq.fetchSumFromTSDB(ctx, store.MetricNameSQLDurationSum, startSecs, endSecs, instance, instanceType, topDigests, topLimited, sumDurationMap); err != nil {
		return err
	}

	sumDurationCountMap := sumMapP.Get()
	defer sumMapP.Put(sumDurationCountMap)
	if err := dq.fetchSumFromTSDB(ctx, store.MetricNameSQLDurationCount, startSecs, endSecs, instance, instanceType, topDigests, topLimited, sumDurationCountMap); err != nil {
		return err
	}

	sumExecMap := sumMapP.Get()
	defer sumMapP.Put(sumExecMap)
	if err := dq.fetchSumFromTSDB(ctx, store.MetricNameSQLExecCount, startSecs, endSecs, instance, instanceType, topDigests, topLimited, sumExecMap); err != nil {
		return err
	}

	sumReadRowMap := sumMapP.Get()
	defer sumMapP.Put(sumReadRowMap)
	if err := dq.fetchSumFromTSDB(ctx, store.MetricNameReadRow, startSecs, endSecs, instance, instanceType, topDigests, topLimited, sumReadRowMap); err != nil {
		return err
	}

	sumReadIndexMap := sumMapP.Get()
	defer sumMapP.Put(sumReadIndexMap)
	if err := dq.fetchSumFromTSDB(ctx, store.MetricNameReadIndex, startSecs, endSecs, instance, instanceType, topDigests, topLimited, sumReadIndexMap); err != nil {
		return err
	}

	var othersItem *SummaryItem
	for i, item := range *fill {
		if err := ctx.Err(); err != nil {
			return err
		}
		if item.IsOther {
			// don't know how it can happen, but we'd better be pessimists
			if len(item.Plans) == 0 {
				item.Plans = append(item.Plans, SummaryPlanItem{})
			}

			othersItem = &(*fill)[i]
			continue
		}

		overallDurationNs := 0.0
		overallDurationCount := 0.0
		overallExecCount := 0.0
		overallReadRows := 0.0
		overallReadIndexes := 0.0
		for j, p := range item.Plans {
			k := RecordKey{SQLDigest: item.SQLDigest, PlanDigest: p.PlanDigest}
			durationNs := sumDurationMap[k]
			delete(sumDurationMap, k)
			durationCount := sumDurationCountMap[k]
			delete(sumDurationCountMap, k)
			execCount := sumExecMap[k]
			delete(sumExecMap, k)
			readRow := sumReadRowMap[k]
			delete(sumReadRowMap, k)
			readIndex := sumReadIndexMap[k]
			delete(sumReadIndexMap, k)

			if durationCount == 0.0 {
				item.Plans[j].DurationPerExecMs = 0
			} else {
				item.Plans[j].DurationPerExecMs = durationNs / 1000000.0 / durationCount
			}
			item.Plans[j].ExecCountPerSec = execCount / rangeSecs
			item.Plans[j].ScanRecordsPerSec = readRow / rangeSecs
			item.Plans[j].ScanIndexesPerSec = readIndex / rangeSecs

			overallDurationNs += durationNs
			overallDurationCount += durationCount
			overallExecCount += execCount
			overallReadRows += readRow
			overallReadIndexes += readIndex
		}

		if overallDurationCount == 0.0 {
			(*fill)[i].DurationPerExecMs = 0
		} else {
			(*fill)[i].DurationPerExecMs = overallDurationNs / 1000000.0 / overallDurationCount
		}
		(*fill)[i].ExecCountPerSec = overallExecCount / rangeSecs
		(*fill)[i].ScanRecordsPerSec = overallReadRows / rangeSecs
		(*fill)[i].ScanIndexesPerSec = overallReadIndexes / rangeSecs
	}

	if othersItem != nil {
		planItem := &(*othersItem).Plans[0]

		othersDurationNs := 0.0
		othersDurationCount := 0.0
		othersExecCount := 0.0
		othersReadRow := 0.0
		othersReadIndex := 0.0
		for _, s := range sumDurationMap {
			othersDurationNs += s
		}
		for _, s := range sumDurationCountMap {
			othersDurationCount += s
		}
		for _, s := range sumExecMap {
			othersExecCount += s
		}
		for _, s := range sumReadRowMap {
			othersReadRow += s
		}
		for _, s := range sumReadIndexMap {
			othersReadIndex += s
		}

		if othersDurationCount == 0.0 {
			othersItem.DurationPerExecMs = 0
			planItem.DurationPerExecMs = othersItem.DurationPerExecMs
		} else {
			othersItem.DurationPerExecMs = othersDurationNs / 1000000.0 / othersDurationCount
			planItem.DurationPerExecMs = othersItem.DurationPerExecMs
		}

		othersItem.ExecCountPerSec = othersExecCount / rangeSecs
		planItem.ExecCountPerSec = othersItem.ExecCountPerSec

		othersItem.ScanRecordsPerSec = othersReadRow / rangeSecs
		planItem.ScanRecordsPerSec = othersItem.ScanRecordsPerSec

		othersItem.ScanIndexesPerSec = othersReadIndex / rangeSecs
		planItem.ScanIndexesPerSec = othersItem.ScanIndexesPerSec
	}

	return ctx.Err()
}
