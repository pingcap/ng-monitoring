package store

type Metric struct {
	Metric     topSQLTags `json:"metric"`
	Timestamps []uint64   `json:"timestamps"` // in millisecond
	Values     []uint32   `json:"values"`
}

type topSQLTags struct {
	Name       string `json:"__name__"`
	Instance   string `json:"instance"`
	Job        string `json:"job"`
	SQLDigest  string `json:"sql_digest"`
	PlanDigest string `json:"plan_digest,omitempty"`
}
