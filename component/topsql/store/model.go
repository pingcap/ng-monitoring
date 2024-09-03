package store

const (
	MetricNameInstance         = "instance"
	MetricNameCPUTime          = "cpu_time"
	MetricNameReadRow          = "read_row"
	MetricNameReadIndex        = "read_index"
	MetricNameWriteRow         = "write_row"
	MetricNameWriteIndex       = "write_index"
	MetricNameSQLExecCount     = "sql_exec_count"
	MetricNameSQLDurationSum   = "sql_duration_sum"
	MetricNameSQLDurationCount = "sql_duration_count"
)

type Metric struct {
	Metric      interface{} `json:"metric"`
	TimestampMs []uint64    `json:"timestamps"`
	Values      []uint64    `json:"values"`
}

type InstanceItem struct {
	Instance     string `json:"instance"`
	InstanceType string `json:"instance_type"`
	TimestampSec uint64 `json:"timestamp"`
}

type recordTags struct {
	Name         string `json:"__name__"`
	Instance     string `json:"instance"`
	InstanceType string `json:"instance_type"`
	SQLDigest    string `json:"sql_digest"`
	PlanDigest   string `json:"plan_digest"`
	DB           string `json:"db"`
	Table        string `json:"table"`
}

type instanceTags struct {
	Name         string `json:"__name__"`
	Instance     string `json:"instance"`
	InstanceType string `json:"instance_type"`
}
