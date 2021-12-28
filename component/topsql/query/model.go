package query

type TopSQLItem struct {
	SQLDigest string     `json:"sql_digest"`
	SQLText   string     `json:"sql_text"`
	Plans     []PlanItem `json:"plans"`
}

type PlanItem struct {
	PlanDigest     string   `json:"plan_digest"`
	PlanText       string   `json:"plan_text"`
	TimestampSecs  []uint64 `json:"timestamp_secs"`
	CPUTimeMillis  []uint64 `json:"cpu_time_millis,omitempty"`
	ReadRows       []uint64 `json:"read_rows,omitempty"`
	ReadIndexes    []uint64 `json:"read_indexes,omitempty"`
	WriteRows      []uint64 `json:"write_rows,omitempty"`
	WriteIndexes   []uint64 `json:"write_indexes,omitempty"`
	SQLExecCount   []uint64 `json:"sql_exec_count,omitempty"`
	SQLDurationSum []uint64 `json:"sql_duration_sum,omitempty"`
	SQLDuration    uint64   `json:"sql_duration,omitempty"`
}

type InstanceItem struct {
	Instance     string `json:"instance"`
	InstanceType string `json:"instance_type"`
}

type metricResp struct {
	Status string         `json:"status"`
	Data   metricRespData `json:"data"`
}

type metricRespData struct {
	ResultType string                 `json:"resultType"`
	Results    []metricRespDataResult `json:"result"`
}

type metricRespDataResult struct {
	Metric metricRespDataResultMetric  `json:"metric"`
	Values []metricRespDataResultValue `json:"values"`
}

type metricRespDataResultMetric struct {
	Instance     string `json:"instance"`
	InstanceType string `json:"instance_type"`
	SQLDigest    string `json:"sql_digest"`
	PlanDigest   string `json:"plan_digest"`
}

type metricRespDataResultValue = []interface{}
