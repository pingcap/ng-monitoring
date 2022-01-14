package meta

const (
	ProfileKindProfile        = "profile"
	ProfileKindGoroutine      = "goroutine"
	ProfileKindHeap           = "heap"
	ProfileKindMutex          = "mutex"
	ProfileDataFormatSVG      = "svg"
	ProfileDataFormatProtobuf = "protobuf"
)

type ProfileStatus int64

const (
	ProfileStatusFinished          ProfileStatus = 0
	ProfileStatusFailed            ProfileStatus = 1
	ProfileStatusRunning           ProfileStatus = 2
	ProfileStatusFinishedWithError ProfileStatus = 3
)

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
	Begin      int64           `json:"begin_time"`
	End        int64           `json:"end_time"`
	Limit      int64           `json:"limit"`
	Targets    []ProfileTarget `json:"targets"`
	DataFormat string          `json:"data_format"`
}

type ProfileList struct {
	Target     ProfileTarget   `json:"target"`
	StatusList []ProfileStatus `json:"status"`
	TsList     []int64         `json:"timestamp_list"`
}

func (s ProfileStatus) String() string {
	switch s {
	case ProfileStatusFinished:
		return "finished"
	case ProfileStatusFailed:
		return "failed"
	case ProfileStatusRunning:
		return "running"
	case ProfileStatusFinishedWithError:
		return "finished_with_error"
	default:
		return "unknown_state"
	}
}
