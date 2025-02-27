package query

import (
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"

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

	return dq.fillText(name, sqlGroups, func(item RecordItem) {
		*fill = append(*fill, item)
	})
}

func (dq *DefaultQuery) SummaryBy(startSecs, endSecs, windowSecs, top int, instance, instanceType, aggBy string, fill *[]SummaryByItem) error {
	if startSecs > endSecs {
		return nil
	}

	// adjust start to make result align to end
	alignStartSecs := endSecs - (endSecs-startSecs)/windowSecs*windowSecs

	var recordsResponse recordsMetricRespV2
	if err := dq.fetchRecordsFromTSDBBy(store.MetricNameCPUTime, alignStartSecs, endSecs, windowSecs, instance, instanceType, aggBy, top, &recordsResponse); err != nil {
		return err
	}
	if len(recordsResponse.Data.Results) == 0 {
		return nil
	}

	for _, result := range recordsResponse.Data.Results {
		text := result.Metric[aggBy].(string)
		sumItem := SummaryByItem{
			Text: text,
		}
		if text == "other" {
			sumItem.IsOther = true
		}
		valueSum := uint64(0)
		for _, value := range result.Values {
			if len(value) != 2 {
				continue
			}

			ts := uint64(value[0].(float64))
			v, err := strconv.ParseUint(value[1].(string), 10, 64)
			if err != nil {
				continue
			}

			valueSum += v
			sumItem.TimestampSec = append(sumItem.TimestampSec, ts)
			sumItem.CPUTimeMs = append(sumItem.CPUTimeMs, v)
		}
		sumItem.CPUTimeMsSum = valueSum
		*fill = append(*fill, sumItem)
	}
	return nil
}

func (dq *DefaultQuery) Summary(startSecs, endSecs, windowSecs, top int, instance, instanceType string, fill *[]SummaryItem) error {
	if startSecs > endSecs {
		return nil
	}

	// adjust start to make result align to end
	alignStartSecs := endSecs - (endSecs-startSecs)/windowSecs*windowSecs

	var recordsResponse recordsMetricResp
	if err := dq.fetchRecordsFromTSDB(store.MetricNameCPUTime, alignStartSecs, endSecs, windowSecs, instance, instanceType, &recordsResponse); err != nil {
		return err
	}
	if len(recordsResponse.Data.Results) == 0 {
		return nil
	}

	sqlGroups := sqlGroupSliceP.Get()
	defer sqlGroupSliceP.Put(sqlGroups)
	topK(recordsResponse.Data.Results, top, sqlGroups)

	if err := dq.fillText(store.MetricNameCPUTime, sqlGroups, func(item RecordItem) {
		sumItem := SummaryItem{
			SQLDigest: item.SQLDigest,
			SQLText:   item.SQLText,
			IsOther:   item.IsOther,
		}

		for _, p := range item.Plans {
			sumItem.Plans = append(sumItem.Plans, SummaryPlanItem{
				PlanDigest:   p.PlanDigest,
				PlanText:     p.PlanText,
				TimestampSec: p.TimestampSec,
				CPUTimeMs:    p.CPUTimeMs,
			})

			for _, cpu := range p.CPUTimeMs {
				sumItem.CPUTimeMs += cpu
			}
		}

		*fill = append(*fill, sumItem)
	}); err != nil {
		return err
	}

	// rangeSecs never to be 0 because startSecs <= endSecs
	rangeSecs := float64(endSecs - startSecs + 1)

	sumDurationMap := sumMapP.Get()
	defer sumMapP.Put(sumDurationMap)
	if err := dq.fetchSumFromTSDB(store.MetricNameSQLDurationSum, startSecs, endSecs, instance, instanceType, sumDurationMap); err != nil {
		return err
	}

	sumDurationCountMap := sumMapP.Get()
	defer sumMapP.Put(sumDurationCountMap)
	if err := dq.fetchSumFromTSDB(store.MetricNameSQLDurationCount, startSecs, endSecs, instance, instanceType, sumDurationCountMap); err != nil {
		return err
	}

	sumExecMap := sumMapP.Get()
	defer sumMapP.Put(sumExecMap)
	if err := dq.fetchSumFromTSDB(store.MetricNameSQLExecCount, startSecs, endSecs, instance, instanceType, sumExecMap); err != nil {
		return err
	}

	sumReadRowMap := sumMapP.Get()
	defer sumMapP.Put(sumReadRowMap)
	if err := dq.fetchSumFromTSDB(store.MetricNameReadRow, startSecs, endSecs, instance, instanceType, sumReadRowMap); err != nil {
		return err
	}

	sumReadIndexMap := sumMapP.Get()
	defer sumMapP.Put(sumReadIndexMap)
	if err := dq.fetchSumFromTSDB(store.MetricNameReadIndex, startSecs, endSecs, instance, instanceType, sumReadIndexMap); err != nil {
		return err
	}

	var othersItem *SummaryItem
	for i, item := range *fill {
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
		for i, p := range item.Plans {
			durationNs := sumDurationMap[RecordKey{SQLDigest: item.SQLDigest, PlanDigest: p.PlanDigest}]
			delete(sumDurationMap, RecordKey{SQLDigest: item.SQLDigest, PlanDigest: p.PlanDigest})
			durationCount := sumDurationCountMap[RecordKey{SQLDigest: item.SQLDigest, PlanDigest: p.PlanDigest}]
			delete(sumDurationCountMap, RecordKey{SQLDigest: item.SQLDigest, PlanDigest: p.PlanDigest})
			execCount := sumExecMap[RecordKey{SQLDigest: item.SQLDigest, PlanDigest: p.PlanDigest}]
			delete(sumExecMap, RecordKey{SQLDigest: item.SQLDigest, PlanDigest: p.PlanDigest})
			readRow := sumReadRowMap[RecordKey{SQLDigest: item.SQLDigest, PlanDigest: p.PlanDigest}]
			delete(sumReadRowMap, RecordKey{SQLDigest: item.SQLDigest, PlanDigest: p.PlanDigest})
			readIndex := sumReadIndexMap[RecordKey{SQLDigest: item.SQLDigest, PlanDigest: p.PlanDigest}]
			delete(sumReadIndexMap, RecordKey{SQLDigest: item.SQLDigest, PlanDigest: p.PlanDigest})

			if durationCount == 0.0 {
				item.Plans[i].DurationPerExecMs = 0
			} else {
				item.Plans[i].DurationPerExecMs = durationNs / 1000000.0 / durationCount
			}
			item.Plans[i].ExecCountPerSec = execCount / rangeSecs
			item.Plans[i].ScanRecordsPerSec = readRow / rangeSecs
			item.Plans[i].ScanIndexesPerSec = readIndex / rangeSecs

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

	return nil
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
	if dq.vmselectHandler == nil {
		return fmt.Errorf("empty query handler")
	}

	bufResp := bytesP.Get()
	header := headerP.Get()

	defer bytesP.Put(bufResp)
	defer headerP.Put(header)

	req, err := http.NewRequest("GET", "/api/v1/query_range", nil)
	if err != nil {
		return err
	}
	reqQuery := req.URL.Query()
	reqQuery.Set("query", fmt.Sprintf("sum_over_time(%s{instance=\"%s\", instance_type=\"%s\"}[%d])", name, instance, instanceType, windowSecs))
	reqQuery.Set("start", strconv.Itoa(startSecs))
	reqQuery.Set("end", strconv.Itoa(endSecs))
	reqQuery.Set("step", strconv.Itoa(windowSecs))
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
	return json.Unmarshal(respR.Body.Bytes(), metricResponse)
}

func (dq *DefaultQuery) fetchRecordsFromTSDBBy(name string, startSecs int, endSecs int, windowSecs int, instance, instanceType, by string, top int, metricResponse *recordsMetricRespV2) error {
	if dq.vmselectHandler == nil {
		return fmt.Errorf("empty query handler")
	}

	bufResp := bytesP.Get()
	header := headerP.Get()

	defer bytesP.Put(bufResp)
	defer headerP.Put(header)

	req, err := http.NewRequest("GET", "/api/v1/query_range", nil)
	if err != nil {
		return err
	}
	reqQuery := req.URL.Query()
	reqQuery.Set("query", fmt.Sprintf("topk_avg(%d, sum(sum_over_time(%s{instance=\"%s\", instance_type=\"%s\"}[%d])) by (%s), \"%s=other\")", top, name, instance, instanceType, windowSecs, by, by))
	reqQuery.Set("start", strconv.Itoa(startSecs))
	reqQuery.Set("end", strconv.Itoa(endSecs))
	reqQuery.Set("step", strconv.Itoa(windowSecs))
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
	return json.Unmarshal(respR.Body.Bytes(), metricResponse)
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

func (dq *DefaultQuery) fetchSumFromTSDB(name string, startSecs, endSecs int, instance, instanceType string, fill map[RecordKey]float64) error {
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
	// Given startSecs = t3, endSecs = t6, to calculate v3 + v4 + v5 + v6
	//
	// The exec model for vm is lookbehind. So we can set time = endSecs, window = endSecs-startSecs+1,
	// to get the point that sum up from v3 to v6.
	//
	// t1  t2  t3  t4  t5  t6  t7  t8  t9
	// v1  v2  v3  v4  v5  v6  v7  v8  v9
	//         ^            ^
	//         |   window   |
	reqQuery.Set("query", fmt.Sprintf("sum_over_time(%s{instance=\"%s\", instance_type=\"%s\"}[%ds])", name, instance, instanceType, endSecs-startSecs+1))
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

	var sumResponse sumMetricResp
	if err = json.Unmarshal(respR.Body.Bytes(), &sumResponse); err != nil {
		return err
	}

	for _, result := range sumResponse.Data.Results {
		if len(result.Value) < 2 {
			continue
		}

		v, ok := result.Value[1].(string)
		if !ok {
			continue
		}

		sum, err := strconv.ParseFloat(v, 64)
		if err != nil {
			continue
		}

		fill[RecordKey{
			SQLDigest:  result.Metric.SQLDigest,
			PlanDigest: result.Metric.PlanDigest,
		}] = sum
	}

	return nil
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
		group := m[r.Metric.SQLDigest]
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

			ts := uint64(value[0].(float64))
			v, err := strconv.ParseUint(value[1].(string), 10, 64)
			if err != nil {
				continue
			}

			group.valueSum += v
			ps.timestampSecs = append(ps.timestampSecs, ts)
			ps.values = append(ps.values, v)
		}

		m[r.Metric.SQLDigest] = group
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

func (dq *DefaultQuery) fillText(name string, sqlGroups *[]sqlGroup, fill func(RecordItem)) error {
	var err error
	ctx := context.Background()
	for _, group := range *sqlGroups {
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
			planDigest := series.planDigest
			var planText string
			var encodedPlan string

			if len(planDigest) != 0 {
				planText, encodedPlan, err = dq.docDB.QueryPlanMeta(ctx, planDigest)
				if err != nil {
					return err
				}
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

	return nil
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
