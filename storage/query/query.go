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

	metricRespP = metricRespPool{}
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

	if err = keepTopK(&result.Data.Results, top); err != nil {
		return err
	}

	return documentDB.View(func(tx *genji.Tx) error {
		for _, res := range result.Data.Results {
			sqlDigest := res.Metric.SQLDigest
			planDigest := res.Metric.PlanDigest

			var sqlText string
			var planText string

			if len(sqlDigest) != 0 {
				r, err := tx.QueryDocument(
					"SELECT sql_text FROM sql_digest WHERE digest = ?",
					sqlDigest,
				)
				if err == nil {
					_ = document.Scan(r, &sqlText)
				}
			}

			if len(planDigest) != 0 {
				r, err := tx.QueryDocument(
					"SELECT plan_text FROM plan_digest WHERE digest = ?",
					planDigest,
				)
				if err == nil {
					_ = document.Scan(r, &planText)
				}
			}

			item := TopSQLItem{
				SQLDigest:  sqlDigest,
				PlanDigest: planDigest,
				SQLText:    sqlText,
				PlanText:   planText,
			}

			for _, value := range res.Values {
				if len(value) != 2 {
					continue
				}

				ts := uint64(value[0].(float64))
				cpu, err := strconv.ParseUint(value[1].(string), 10, 64)
				if err != nil {
					continue
				}

				item.TimestampSecs = append(item.TimestampSecs, ts)
				item.CPUTimeMillis = append(item.CPUTimeMillis, uint32(cpu))
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

func keepTopK(results *[]metricRespDataResult, top int) error {
	if top <= 0 || len(*results) <= top {
		return nil
	}

	for i := range *results {
		r := &(*results)[i]
		var sum uint64
		for _, value := range r.Values {
			if len(value) != 2 {
				continue
			}

			cpu, err := strconv.ParseUint(value[1].(string), 10, 64)
			if err != nil {
				continue
			}

			sum += cpu
		}
		r.sum = sum
	}

	if err := quickselect.QuickSelect(TopKSlice{s:*results}, top); err != nil {
		return err
	}

	*results = (*results)[:top]

	return nil
}


type TopKSlice struct {
	s []metricRespDataResult
}

func (s TopKSlice) Len() int {
	return len(s.s)
}

func (s TopKSlice) Less(i, j int) bool {
	return s.s[i].sum > s.s[j].sum
}

func (s TopKSlice) Swap(i, j int) {
	s.s[i], s.s[j] = s.s[j], s.s[i]
}
