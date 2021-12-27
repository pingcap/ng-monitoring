package query

type TopSQLItem struct {
	SQLDigest string     `json:"sql_digest"`
	SQLText   string     `json:"sql_text"`
	Plans     []PlanItem `json:"plans"`
}

type PlanItem struct {
	PlanDigest    string   `json:"plan_digest"`
	PlanText      string   `json:"plan_text"`
	TimestampSecs []uint64 `json:"timestamp_secs"`
	CPUTimeMillis []uint32 `json:"cpu_time_millis,omitempty"`
	ReadRows      []uint32 `json:"read_rows,omitempty"`
	ReadIndexes   []uint32 `json:"read_indexes,omitempty"`
	WriteRows     []uint32 `json:"write_rows,omitempty"`
	WriteIndexes  []uint32 `json:"write_indexes,omitempty"`
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
