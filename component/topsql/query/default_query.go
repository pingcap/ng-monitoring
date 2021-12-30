package query

import (
	"container/heap"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
	"github.com/pingcap/log"
	"github.com/pingcap/ng-monitoring/component/topsql/store"
	"github.com/pingcap/ng-monitoring/utils"
	"github.com/wangjohn/quickselect"
	"go.uber.org/zap"
)

var (
	bytesP  = utils.BytesBufferPool{}
	headerP = utils.HeaderPool{}

	recordsRespP   = recordsRespPool{}
	sqlGroupSliceP = sqlGroupSlicePool{}
	sqlDigestMapP  = sqlDigestMapPool{}
)

type DefaultQuery struct {
	vmselectHandler http.HandlerFunc
	documentDB      *genji.DB
}

func NewDefaultQuery(vmselectHandler http.HandlerFunc, documentDB *genji.DB) *DefaultQuery {
	return &DefaultQuery{
		vmselectHandler: vmselectHandler,
		documentDB:      documentDB,
	}
}

var _ Query = &DefaultQuery{}

func (dq *DefaultQuery) Records(name string, startSecs, endSecs, windowSecs, top int, instance, instanceType string, fill *[]RecordItem) error {
	if startSecs > endSecs {
		return nil
	}

	// adjust start to make result align to end
	startSecs = endSecs - (endSecs-startSecs)/windowSecs*windowSecs

	recordsResponse := recordsRespP.Get()
	defer recordsRespP.Put(recordsResponse)

	if err := dq.fetchRecordsFromTSDB(name, startSecs, endSecs, windowSecs, instance, instanceType, recordsResponse); err != nil {
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

func (dq *DefaultQuery) Summary(startSecs, endSecs, windowSecs, top int, instance, instanceType string, fill *[]SummaryItem) error {
	if startSecs > endSecs {
		return nil
	}

	// adjust start to make result align to end
	alignStartSecs := endSecs - (endSecs-startSecs)/windowSecs*windowSecs

	recordsResponse := recordsRespP.Get()
	defer recordsRespP.Put(recordsResponse)
	if err := dq.fetchRecordsFromTSDB(store.MetricNameCPUTime, alignStartSecs, endSecs, windowSecs, instance, instanceType, recordsResponse); err != nil {
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
		}

		*fill = append(*fill, sumItem)
	}); err != nil {
		return err
	}

	// rangeSecs never to be 0 because startSecs <= endSecs
	rangeSecs := float64(endSecs - startSecs + 1)
	for _, item := range *fill {
		if item.IsOther {
			continue
		}
		for i := range item.Plans {
			planDigest := item.Plans[i].PlanDigest
			sumDurationNs, err := dq.fetchSumFromTSDB(store.MetricNameSQLDurationSum, startSecs, endSecs, instance, instanceType, item.SQLDigest, planDigest)
			if err != nil {
				return err
			}
			sumExecCount, err := dq.fetchSumFromTSDB(store.MetricNameSQLExecCount, startSecs, endSecs, instance, instanceType, item.SQLDigest, planDigest)
			if err != nil {
				return err
			}
			sumReadRows, err := dq.fetchSumFromTSDB(store.MetricNameReadRow, startSecs, endSecs, instance, instanceType, item.SQLDigest, planDigest)
			if err != nil {
				return err
			}
			sumReadIndexes, err := dq.fetchSumFromTSDB(store.MetricNameReadIndex, startSecs, endSecs, instance, instanceType, item.SQLDigest, planDigest)
			if err != nil {
				return err
			}

			if sumExecCount == 0.0 {
				item.Plans[i].DurationPerExecMs = 0
			} else {
				item.Plans[i].DurationPerExecMs = sumDurationNs / 1000000.0 / sumExecCount
			}
			item.Plans[i].ExecCountPerSec = sumExecCount / rangeSecs
			item.Plans[i].ScanRecordsPerSec = sumReadRows / rangeSecs
			item.Plans[i].ScanIndexesPerSec = sumReadIndexes / rangeSecs
		}
	}

	return nil
}

func (dq *DefaultQuery) Instances(startSecs, endSecs int, fill *[]InstanceItem) error {
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
	req.URL.RawQuery = reqQuery.Encode()
	req.Header.Set("Accept", "application/json")

	respR := utils.NewRespWriter(bufResp, header)
	dq.vmselectHandler(&respR, req)

	if statusOK := respR.Code >= 200 && respR.Code < 300; !statusOK {
		log.Warn("failed to fetch timeseries db", zap.String("error", respR.Body.String()))
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
	req, err := http.NewRequest("GET", "/api/v1/query_range", nil)
	if err != nil {
		return err
	}
	reqQuery := req.URL.Query()
	reqQuery.Set("query", "last_over_time(instance)")
	reqQuery.Set("start", strconv.Itoa(startSecs))
	reqQuery.Set("end", strconv.Itoa(endSecs))
	reqQuery.Set("step", strconv.Itoa(endSecs-startSecs))
	req.URL.RawQuery = reqQuery.Encode()
	req.Header.Set("Accept", "application/json")
	respR := utils.NewRespWriter(bufResp, header)
	dq.vmselectHandler(&respR, req)
	if statusOK := respR.Code >= 200 && respR.Code < 300; !statusOK {
		log.Warn("failed to fetch timeseries db", zap.String("error", respR.Body.String()))
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

func (dq *DefaultQuery) fetchSumFromTSDB(name string, startSecs, endSecs int, instance, instanceType, sqlDigest, planDigest string) (float64, error) {
	if dq.vmselectHandler == nil {
		return 0, fmt.Errorf("empty query handler")
	}

	bufResp := bytesP.Get()
	header := headerP.Get()

	defer bytesP.Put(bufResp)
	defer headerP.Put(header)

	req, err := http.NewRequest("GET", "/api/v1/query_range", nil)
	if err != nil {
		return 0, err
	}
	reqQuery := req.URL.Query()
	reqQuery.Set("query", fmt.Sprintf("sum_over_time(%s{instance=\"%s\", instance_type=\"%s\", sql_digest=\"%s\", plan_digest=\"%s\"})", name, instance, instanceType, sqlDigest, planDigest))

	// Data:
	// t1  t2  t3  t4  t5  t6  t7  t8  t9
	// v1  v2  v3  v4  v5  v6  v7  v8  v9
	//
	// Given startSecs = t3, endSecs = t6, to calculate v3 + v4 + v5 + v6
	//
	// The exec model for vm is lookbehind. So we can set start = endSecs, end = endSecs, step = endSecs-startSecs+1,
	// to get the point that sum up from v3 to v6.
	//
	// t1  t2  t3  t4  t5  t6  t7  t8  t9
	// v1  v2  v3  v4  v5  v6  v7  v8  v9
	//         ^            ^
	//         | -- step -- |
	reqQuery.Set("start", strconv.Itoa(endSecs))
	reqQuery.Set("end", strconv.Itoa(endSecs))
	reqQuery.Set("step", strconv.Itoa(endSecs-startSecs+1))
	req.URL.RawQuery = reqQuery.Encode()
	req.Header.Set("Accept", "application/json")

	respR := utils.NewRespWriter(bufResp, header)
	dq.vmselectHandler(&respR, req)

	if statusOK := respR.Code >= 200 && respR.Code < 300; !statusOK {
		log.Warn("failed to fetch timeseries db", zap.String("error", respR.Body.String()))
	}

	recordsResponse := recordsRespP.Get()
	defer recordsRespP.Put(recordsResponse)
	if err = json.Unmarshal(respR.Body.Bytes(), recordsResponse); err != nil {
		return 0, err
	}

	if len(recordsResponse.Data.Results) > 0 {
		res := recordsResponse.Data.Results[0]
		if len(res.Values) > 0 {
			pair := res.Values[0]
			if len(pair) >= 2 {
				if v, ok := pair[1].(string); ok {
					return strconv.ParseFloat(v, 64)
				}
			}
		}
	}

	return 0, nil
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
	return dq.documentDB.View(func(tx *genji.Tx) error {
		for _, group := range *sqlGroups {
			sqlDigest := group.sqlDigest
			var sqlText string

			if len(sqlDigest) != 0 {
				r, err := tx.QueryDocument(
					"SELECT sql_text FROM sql_digest WHERE digest = ?",
					sqlDigest,
				)
				if err == nil {
					_ = document.Scan(r, &sqlText)
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

				if len(planDigest) != 0 {
					r, err := tx.QueryDocument(
						"SELECT plan_text FROM plan_digest WHERE digest = ?",
						planDigest,
					)
					if err == nil {
						_ = document.Scan(r, &planText)
					}
				}

				planItem := RecordPlanItem{
					PlanDigest:   planDigest,
					PlanText:     planText,
					TimestampSec: series.timestampSecs,
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
				}
				item.Plans = append(item.Plans, planItem)
			}

			fill(item)
		}

		return nil
	})
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
