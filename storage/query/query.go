package query

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/types"
	"github.com/go-resty/resty/v2"
	"github.com/pingcap/log"
	"github.com/wangjohn/quickselect"
)

var (
	client = resty.New()

	metricsAPIURL = ""
	documentDB    *genji.DB

	metricRespP    = metricRespPool{}
	sqlGroupSliceP = sqlGroupSlicePool{}
	sqlDigestMapP  = sqlDigestMapPool{}
)

func Init(httpAddr string, db *genji.DB) {
	initMetricsAPIURL(httpAddr)
	documentDB = db
}

func initMetricsAPIURL(httpAddr string) {
	if len(httpAddr) == 0 {
		log.Fatal("empty listen addr")
	}

	if (httpAddr)[0] == ':' {
		metricsAPIURL = fmt.Sprintf("http://0.0.0.0%s/api/v1/query_range", httpAddr)
		return
	}

	metricsAPIURL = fmt.Sprintf("http://%s/api/v1/query_range", httpAddr)
}

func TopSQL(startSecs, endSecs, windowSecs, top int, instance string, fill *[]TopSQLItem) error {
	query := fmt.Sprintf("sum_over_time(cpu_time{instance=\"%s\"}[%d])", instance, windowSecs)

	metricResponse := metricRespP.Get()
	defer metricRespP.Put(metricResponse)

	start := strconv.Itoa(startSecs - startSecs%windowSecs)
	end := strconv.Itoa(endSecs - endSecs%windowSecs + windowSecs)
	resp, err := client.R().
		SetQueryParam("query", query).
		SetQueryParam("start", start).
		SetQueryParam("end", end).
		SetQueryParam("step", strconv.Itoa(windowSecs)).
		SetResult(metricResponse).
		SetHeader("Accept", "application/json").
		Get(metricsAPIURL)
	if err != nil {
		return err
	}

	result, ok := resp.Result().(*metricResp)
	if !ok {
		return errors.New("failed to decode timeseries data")
	}

	sqlGroups := sqlGroupSliceP.Get()
	defer sqlGroupSliceP.Put(sqlGroups)
	groupBySQLDigest(result.Data.Results, sqlGroups)

	if err = keepTopK(sqlGroups, top); err != nil {
		return err
	}

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

				item.Plans = append(item.Plans, PlanItem{
					PlanDigest:    planDigest,
					PlanText:      planText,
					TimestampSecs: series.timestampSecs,
					CPUTimeMillis: series.cpuTimeMillis,
				})
			}

			*fill = append(*fill, item)
		}

		return nil
	})
}

func AllInstances(fill *[]InstanceItem) error {
	doc, err := documentDB.Query("SELECT instance, job FROM instance")
	if err != nil {
		return err
	}
	defer doc.Close()

	return doc.Iterate(func(d types.Document) error {
		item := InstanceItem{}

		err := document.Scan(d, &item.Instance, &item.Job)
		if err != nil {
			return err
		}

		*fill = append(*fill, item)
		return nil
	})
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

type planSeries struct {
	planDigest    string
	timestampSecs []uint64
	cpuTimeMillis []uint32
}

type sqlGroup struct {
	sqlDigest  string
	planSeries []planSeries
	cpuTimeSum uint32
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
			cpu, err := strconv.ParseUint(value[1].(string), 10, 64)
			if err != nil {
				continue
			}

			group.cpuTimeSum += uint32(cpu)
			ps.timestampSecs = append(ps.timestampSecs, ts)
			ps.cpuTimeMillis = append(ps.cpuTimeMillis, uint32(cpu))
		}

		m[r.Metric.SQLDigest] = group
	}

	for _, group := range m {
		*target = append(*target, sqlGroup{
			sqlDigest:  group.sqlDigest,
			planSeries: group.planSeries,
			cpuTimeSum: group.cpuTimeSum,
		})
	}
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

	if si.cpuTimeSum != sj.cpuTimeSum {
		return si.cpuTimeSum > sj.cpuTimeSum
	}

	return si.sqlDigest > sj.sqlDigest
}

func (s TopKSlice) Swap(i, j int) {
	s.s[i], s.s[j] = s.s[j], s.s[i]
}
