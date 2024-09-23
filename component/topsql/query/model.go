package query

type RecordKey struct {
	SQLDigest  string
	PlanDigest string
}

type RecordItem struct {
	SQLDigest string           `json:"sql_digest"`
	SQLText   string           `json:"sql_text"`
	IsOther   bool             `json:"is_other"`
	Plans     []RecordPlanItem `json:"plans"`
}

type RecordPlanItem struct {
	PlanDigest       string   `json:"plan_digest"`
	PlanText         string   `json:"plan_text"`
	TimestampSec     []uint64 `json:"timestamp_sec"`
	CPUTimeMs        []uint64 `json:"cpu_time_ms,omitempty"`
	ReadRows         []uint64 `json:"read_rows,omitempty"`
	ReadIndexes      []uint64 `json:"read_indexes,omitempty"`
	WriteRows        []uint64 `json:"write_rows,omitempty"`
	WriteIndexes     []uint64 `json:"write_indexes,omitempty"`
	SQLExecCount     []uint64 `json:"sql_exec_count,omitempty"`
	SQLDurationSum   []uint64 `json:"sql_duration_sum,omitempty"`
	SQLDurationCount []uint64 `json:"sql_duration_count,omitempty"`
}

type SummaryByItem struct {
	Text         string   `json:"text"`
	TimestampSec []uint64 `json:"timestamp_sec"`
	CPUTimeMs    []uint64 `json:"cpu_time_ms,omitempty"`
	CPUTimeMsSum uint64   `json:"cpu_time_ms_sum"`
	IsOther      bool     `json:"is_other"`
}

type SummaryItem struct {
	SQLDigest         string            `json:"sql_digest"`
	SQLText           string            `json:"sql_text"`
	IsOther           bool              `json:"is_other"`
	CPUTimeMs         uint64            `json:"cpu_time_ms"`
	ExecCountPerSec   float64           `json:"exec_count_per_sec"`
	DurationPerExecMs float64           `json:"duration_per_exec_ms"`
	ScanRecordsPerSec float64           `json:"scan_records_per_sec"`
	ScanIndexesPerSec float64           `json:"scan_indexes_per_sec"`
	Plans             []SummaryPlanItem `json:"plans"`
}

type SummaryPlanItem struct {
	PlanDigest        string   `json:"plan_digest"`
	PlanText          string   `json:"plan_text"`
	TimestampSec      []uint64 `json:"timestamp_sec"`
	CPUTimeMs         []uint64 `json:"cpu_time_ms,omitempty"`
	ExecCountPerSec   float64  `json:"exec_count_per_sec"`
	DurationPerExecMs float64  `json:"duration_per_exec_ms"`
	ScanRecordsPerSec float64  `json:"scan_records_per_sec"`
	ScanIndexesPerSec float64  `json:"scan_indexes_per_sec"`
}

type InstanceItem struct {
	Instance     string `json:"instance"`
	InstanceType string `json:"instance_type"`
}

type recordsMetricResp struct {
	Status string                `json:"status"`
	Data   recordsMetricRespData `json:"data"`
}

type recordsMetricRespData struct {
	ResultType string                        `json:"resultType"`
	Results    []recordsMetricRespDataResult `json:"result"`
}

type recordsMetricRespDataResult struct {
	Metric recordsMetricRespDataResultMetric  `json:"metric"`
	Values []recordsMetricRespDataResultValue `json:"values"`
}

type recordsMetricRespDataResultMetric struct {
	Instance     string `json:"instance"`
	InstanceType string `json:"instance_type"`
	SQLDigest    string `json:"sql_digest"`
	PlanDigest   string `json:"plan_digest"`
}

type recordsMetricRespV2 struct {
	Status string                  `json:"status"`
	Data   recordsMetricRespDataV2 `json:"data"`
}

type recordsMetricRespDataV2 struct {
	ResultType string                          `json:"resultType"`
	Results    []recordsMetricRespDataResultV2 `json:"result"`
}

type recordsMetricRespDataResultV2 struct {
	Metric map[string]interface{}             `json:"metric"`
	Values []recordsMetricRespDataResultValue `json:"values"`
}

type recordsMetricRespDataResultValue = []interface{}

type instancesMetricResp struct {
	Status string                  `json:"status"`
	Data   instancesMetricRespData `json:"data"`
}

type instancesMetricRespData struct {
	ResultType string                          `json:"resultType"`
	Results    []instancesMetricRespDataResult `json:"result"`
}

type instancesMetricRespDataResult struct {
	Metric instancesMetricRespDataResultMetric `json:"metric"`
}

type instancesMetricRespDataResultMetric struct {
	Instance     string `json:"instance"`
	InstanceType string `json:"instance_type"`
}

type sumMetricResp struct {
	Status string            `json:"status"`
	Data   sumMetricRespData `json:"data"`
}

type sumMetricRespData struct {
	ResultType string                    `json:"resultType"`
	Results    []sumMetricRespDataResult `json:"result"`
}

type sumMetricRespDataResult struct {
	Metric sumMetricRespDataResultMetric `json:"metric"`
	Value  []interface{}                 `json:"value"`
}

type sumMetricRespDataResultMetric struct {
	SQLDigest  string `json:"sql_digest"`
	PlanDigest string `json:"plan_digest"`
}
