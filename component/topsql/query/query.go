package query

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/pingcap/ng-monitoring/component/topsql/store"
	"github.com/pingcap/ng-monitoring/utils"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/types"
	"github.com/pingcap/log"
	"github.com/wangjohn/quickselect"
	"go.uber.org/zap"
)

var (
	vmselectHandler http.HandlerFunc
	documentDB      *genji.DB

	bytesP  = utils.BytesBufferPool{}
	headerP = utils.HeaderPool{}

	metricRespP    = metricRespPool{}
	sqlGroupSliceP = sqlGroupSlicePool{}
	sqlDigestMapP  = sqlDigestMapPool{}
)

func Init(vmselectHandler_ http.HandlerFunc, db *genji.DB) {
	vmselectHandler = vmselectHandler_
	documentDB = db
}

func Stop() {

}

func TopSQL(name string, startSecs, endSecs, windowSecs, top int, instance string, fill *[]TopSQLItem) error {
	metricResponse := metricRespP.Get()
	defer metricRespP.Put(metricResponse)
	if err := fetchTimeseriesDB(name, startSecs, endSecs, windowSecs, instance, metricResponse); err != nil {
		return err
	}

	sqlGroups := sqlGroupSliceP.Get()
	defer sqlGroupSliceP.Put(sqlGroups)
	if err := topK(metricResponse.Data.Results, top, sqlGroups); err != nil {
		return err
	}

	return fillText(name, sqlGroups, fill)
}

func AllInstances(fill *[]InstanceItem) error {
	doc, err := documentDB.Query("SELECT instance, instance_type FROM instance")
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

func fetchTimeseriesDB(name string, startSecs int, endSecs int, windowSecs int, instance string, metricResponse *metricResp) error {
	if vmselectHandler == nil {
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
	vmselectHandler(&respR, req)

	if statusOK := respR.Code >= 200 && respR.Code < 300; !statusOK {
		log.Warn("failed to fetch timeseries db", zap.String("error", respR.Body.String()))
	}

	return json.Unmarshal(respR.Body.Bytes(), metricResponse)
}

func topK(results []metricRespDataResult, top int, sqlGroups *[]sqlGroup) error {
	groupBySQLDigest(results, sqlGroups)
	if err := keepTopK(sqlGroups, top); err != nil {
		return err
	}
	return nil
}

func groupBySQLDigest(resp []metricRespDataResult, target *[]sqlGroup) {
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

func fillText(name string, sqlGroups *[]sqlGroup, fill *[]TopSQLItem) error {
	return documentDB.View(func(tx *genji.Tx) error {
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
