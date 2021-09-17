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

func TopSQL(startSecs, endSecs, windowSecs, top int64, instance string, fill *[]TopSQLItem) error {
    var query string
    if top == -1 {
        query = fmt.Sprintf("sum_over_time(cpu_time{instance=\"%s\"}[%d])", instance, windowSecs)
    } else {
        query = fmt.Sprintf("topk(%d, sum_over_time(cpu_time{instance=\"%s\"}[%d]))", top, instance, windowSecs)
    }

    metricResponse := metricRespP.Get()
    defer metricRespP.Put(metricResponse)

    resp, err := client.R().
        SetQueryParam("query", query).
        SetQueryParam("start", strconv.Itoa(int(startSecs))).
        SetQueryParam("end", strconv.Itoa(int(endSecs))).
        SetQueryParam("step", strconv.Itoa(int(windowSecs))).
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

    return documentDB.View(func(tx *genji.Tx) error {
        for _, res := range result.Data.Results {
            var sqlText string
            var planText string

            sqlDigest := res.Metric.SQLDigest
            planDigest := res.Metric.PlanDigest

            if len(sqlDigest) != 0 {
                r, err := documentDB.QueryDocument(
                    "SELECT sql_text FROM sql_digest WHERE digest = ?",
                    sqlDigest,
                )
                if err != nil {
                    continue
                }

                err = document.Scan(r, &sqlText)
                if err != nil {
                    continue
                }
            }

            if len(planDigest) != 0 {
                r, err := documentDB.QueryDocument(
                    "SELECT plan_text FROM plan_digest WHERE digest = ?",
                    planDigest,
                )
                if err != nil {
                    continue
                }

                err = document.Scan(r, &planText)
                if err != nil {
                    continue
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
