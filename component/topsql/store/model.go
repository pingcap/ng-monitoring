package store

const (
	MetricNameInstance   = "instance"
	MetricNameCPUTime    = "cpu_time"
	MetricNameReadRow    = "read_row"
	MetricNameReadIndex  = "read_index"
	MetricNameWriteRow   = "write_row"
	MetricNameWriteIndex = "write_index"
)

type Metric struct {
	Metric     interface{} `json:"metric"`
	Timestamps []uint64    `json:"timestamps"` // in millisecond
	Values     []uint32    `json:"values"`
}

type InstanceItem struct {
	Instance      string `json:"instance"`
	InstanceType  string `json:"instance_type"`
	TimestampSecs uint64 `json:"timestamp"`
}

type recordTags struct {
	Name         string `json:"__name__"`
	Instance     string `json:"instance"`
	InstanceType string `json:"instance_type"`
	SQLDigest    string `json:"sql_digest"`
	PlanDigest   string `json:"plan_digest,omitempty"`
}

type instanceTags struct {
	Name         string `json:"__name__"`
	Instance     string `json:"instance"`
	InstanceType string `json:"instance_type"`
}
