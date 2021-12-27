package query

import (
	"encoding/json"
	"fmt"
	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
	"github.com/pingcap/log"
	"github.com/pingcap/ng-monitoring/component/topsql/store"
	"github.com/pingcap/ng-monitoring/utils"
	"github.com/wangjohn/quickselect"
	"go.uber.org/zap"
	"net/http"
	"strconv"
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

func (dq *DefaultQuery) TopSQL(name string, startSecs, endSecs, windowSecs, top int, instance string, fill *[]TopSQLItem) error {
	metricResponse := metricRespP.Get()
	defer metricRespP.Put(metricResponse)
	if err := dq.fetchRecordsFromTSDB(name, startSecs, endSecs, windowSecs, instance, metricResponse); err != nil {
		return err
	}

	sqlGroups := sqlGroupSliceP.Get()
	defer sqlGroupSliceP.Put(sqlGroups)
	if err := topK(metricResponse.Data.Results, top, sqlGroups); err != nil {
		return err
	}

	return dq.fillText(name, sqlGroups, fill)
}

func (dq *DefaultQuery) Instances(startSecs, endSecs int, fill *[]InstanceItem) error {
	return dq.fetchInstanceFromTSDB(startSecs, endSecs, fill)
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

func (dq *DefaultQuery) fetchRecordsFromTSDB(name string, startSecs, endSecs, windowSecs int, instance string, metricResponse *recordsMetricResp) error {
	if dq.vmselectHandler == nil {
		return fmt.Errorf("empty query handler")
	}

	bufResp := bytesP.Get()
	header := headerP.Get()

	defer bytesP.Put(bufResp)
	defer headerP.Put(header)

	query := fmt.Sprintf("sum_over_time(%s{instance=\"%s\"}[%d])", name, instance, windowSecs)
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

func (dq *DefaultQuery) fetchInstanceFromTSDB(startSecs, endSecs int, fill *[]InstanceItem) error {
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

func topK(results []recordsMetricRespDataResult, top int, sqlGroups *[]sqlGroup) error {
	groupBySQLDigest(results, sqlGroups)
	if err := keepTopK(sqlGroups, top); err != nil {
		return err
	}
	return nil
}

func groupBySQLDigest(resp []recordsMetricRespDataResult, target *[]sqlGroup) {
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
		*target = append(*target, sqlGroup{
			sqlDigest:  group.sqlDigest,
			planSeries: group.planSeries,
			valueSum:   group.valueSum,
		})
	}
}

func keepTopK(groups *[]sqlGroup, top int) error {
	if top <= 0 || len(*groups) <= top {
		return nil
	}

	if err := quickselect.QuickSelect(TopKSlice{s: *groups}, top); err != nil {
		return err
	}

	*groups = (*groups)[:top]

	return nil
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

type TopKSlice struct {
	s []sqlGroup
}

func (s TopKSlice) Len() int {
	return len(s.s)
}

func (s TopKSlice) Less(i, j int) bool {
	si := s.s[i]
	sj := s.s[j]

	if si.valueSum != sj.valueSum {
		return si.valueSum > sj.valueSum
	}

	return si.sqlDigest > sj.sqlDigest
}

func (s TopKSlice) Swap(i, j int) {
	s.s[i], s.s[j] = s.s[j], s.s[i]
}
