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
	"github.com/genjidb/genji/types"
	"github.com/pingcap/log"
	"github.com/pingcap/ng-monitoring/component/topsql/store"
	"github.com/pingcap/ng-monitoring/utils"
	"github.com/wangjohn/quickselect"
	"go.uber.org/zap"
)

var (
	bytesP  = utils.BytesBufferPool{}
	headerP = utils.HeaderPool{}

	metricRespP    = metricRespPool{}
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

func (dq *DefaultQuery) TopSQL(name string, startSecs, endSecs, windowSecs, top int, instance, instanceType string, fill *[]TopSQLItem) error {
	metricResponse := metricRespP.Get()
	defer metricRespP.Put(metricResponse)
	if err := dq.fetchTimeseriesDB(name, startSecs, endSecs, windowSecs, instance, instanceType, metricResponse); err != nil {
		return err
	}

	sqlGroups := sqlGroupSliceP.Get()
	defer sqlGroupSliceP.Put(sqlGroups)
	topK(metricResponse.Data.Results, top, sqlGroups)

	return dq.fillText(name, sqlGroups, fill)
}

func (dq *DefaultQuery) AllInstances(fill *[]InstanceItem) error {
	doc, err := dq.documentDB.Query("SELECT instance, instance_type FROM instance")
	if err != nil {
		return err
	}
	defer doc.Close()

	return doc.Iterate(func(d types.Document) error {
		item := InstanceItem{}

		err := document.Scan(d, &item.Instance, &item.InstanceType)
		if err != nil {
			return err
		}

		*fill = append(*fill, item)
		return nil
	})
}

func (dq *DefaultQuery) Close() {}

type planSeries struct {
	planDigest    string
	timestampSecs []uint64
	values        []uint32
}

type sqlGroup struct {
	sqlDigest  string
	planSeries []planSeries
	valueSum   uint32
}

func (dq *DefaultQuery) fetchTimeseriesDB(name string, startSecs int, endSecs int, windowSecs int, instance, instanceType string, metricResponse *metricResp) error {
	if dq.vmselectHandler == nil {
		return fmt.Errorf("empty query handler")
	}

	bufResp := bytesP.Get()
	header := headerP.Get()

	defer bytesP.Put(bufResp)
	defer headerP.Put(header)

	query := fmt.Sprintf("sum_over_time(%s{instance=\"%s\", instance_type=\"%s\"}[%d])", name, instance, instanceType, windowSecs)
	start := strconv.Itoa(startSecs - startSecs%windowSecs)
	end := strconv.Itoa(endSecs - endSecs%windowSecs + windowSecs)

	req, err := http.NewRequest("GET", "/api/v1/query_range", nil)
	if err != nil {
		return err
	}
	reqQuery := req.URL.Query()
	reqQuery.Set("query", query)
	reqQuery.Set("start", start)
	reqQuery.Set("end", end)
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

func topK(results []metricRespDataResult, top int, sqlGroups *[]sqlGroup) {
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
func groupBySQLDigest(resp []metricRespDataResult, target *[]sqlGroup) (others sqlGroup) {
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

			group.valueSum += uint32(v)
			ps.timestampSecs = append(ps.timestampSecs, ts)
			ps.values = append(ps.values, uint32(v))
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

func (dq *DefaultQuery) fillText(name string, sqlGroups *[]sqlGroup, fill *[]TopSQLItem) error {
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

			item := TopSQLItem{
				SQLDigest: sqlDigest,
				SQLText:   sqlText,
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

				planItem := PlanItem{
					PlanDigest:    planDigest,
					PlanText:      planText,
					TimestampSecs: series.timestampSecs,
				}
				switch name {
				case store.MetricNameCPUTime:
					planItem.CPUTimeMillis = series.values
				case store.MetricNameReadRow:
					planItem.ReadRows = series.values
				case store.MetricNameReadIndex:
					planItem.ReadIndexes = series.values
				case store.MetricNameWriteRow:
					planItem.WriteRows = series.values
				case store.MetricNameWriteIndex:
					planItem.WriteIndexes = series.values
				}
				item.Plans = append(item.Plans, planItem)
			}

			*fill = append(*fill, item)
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
	v             uint32
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
