package meta

type ProfileTarget struct {
	Kind      string `json:"kind"`
	Component string `json:"component"`
	Address   string `json:"address"`
}

type TargetInfo struct {
	ID           int64
	LastScrapeTs int64
}

type BasicQueryParam struct {
	Begin   int64           `json:"begin_time"`
	End     int64           `json:"end_time"`
	Targets []ProfileTarget `json:"targets"`
}

type ProfileList struct {
	Target ProfileTarget `json:"target"`
	TsList []int64       `json:"timestamp_list"`
}
