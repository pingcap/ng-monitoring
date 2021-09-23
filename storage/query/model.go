package query

type TopSQLItem struct {
	SQLDigest     string   `json:"sql_digest"`
	PlanDigest    string   `json:"plan_digest"`
	SQLText       string   `json:"sql_text"`
	PlanText      string   `json:"plan_text"`
	TimestampSecs []uint64 `json:"timestamp_secs"`
	CPUTimeMillis []uint32 `json:"cpu_time_millis"`
}

type InstanceItem struct {
	Instance string `json:"instance"`
	Job      string `json:"job"`
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
	Instance   string `json:"instance"`
	Job        string `json:"job"`
	SQLDigest  string `json:"sql_digest"`
	PlanDigest string `json:"plan_digest"`
}

type metricRespDataResultValue = []interface{}
