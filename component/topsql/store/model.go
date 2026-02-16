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
	MetricNameSQLNetworkIn     = "sql_network_in_bytes"
	MetricNameSQLNetworkOut    = "sql_network_out_bytes"

	MetricNameNetworkInBytes    = "network_in_bytes"
	MetricNameNetworkOutBytes   = "network_out_bytes"
	MetricNameLogicalReadBytes  = "logical_read_bytes"
	MetricNameLogicalWriteBytes = "logical_write_bytes"

	MetricNameRegionCPUTime           = "region_cpu_time"
	MetricNameRegionReadKeys          = "region_read_keys"
	MetricNameRegionWriteKeys         = "region_write_keys"
	MetricNameRegionNetworkInBytes    = "region_network_in_bytes"
	MetricNameRegionNetworkOutBytes   = "region_network_out_bytes"
	MetricNameRegionLogicalReadBytes  = "region_logical_read_bytes"
	MetricNameRegionLogicalWriteBytes = "region_logical_write_bytes"
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

type regionTags struct {
	Name         string `json:"__name__"`
	Instance     string `json:"instance"`
	InstanceType string `json:"instance_type"`
	RegionID     string `json:"region_id"`
}
