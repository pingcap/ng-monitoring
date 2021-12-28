package store

const (
	MetricNameCPUTime        = "cpu_time"
	MetricNameReadRow        = "read_row"
	MetricNameReadIndex      = "read_index"
	MetricNameWriteRow       = "write_row"
	MetricNameWriteIndex     = "write_index"
	MetricNameSQLExecCount   = "sql_exec_count"
	MetricNameSQLDurationSum = "sql_duration_sum"
)

const (
	// VirtualMetricNameSQLDuration is obtained by dividing DurationSum by SQLExecCount.
	//
	// VirtualMetricXxx means that these metrics do not have corresponding data in tsdb,
	// but are calculated by MetricXxx.
	VirtualMetricNameSQLDuration = "sql_duration"
)

type Metric struct {
	Metric     topSQLTags `json:"metric"`
	Timestamps []uint64   `json:"timestamps"` // in millisecond
	Values     []uint64   `json:"values"`
}

type topSQLTags struct {
	Name         string `json:"__name__"`
	Instance     string `json:"instance"`
	InstanceType string `json:"instance_type"`
	SQLDigest    string `json:"sql_digest"`
	PlanDigest   string `json:"plan_digest,omitempty"`
}
