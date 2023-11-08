package meta

const (
	ProfileKindProfile        = "profile"
	ProfileKindGoroutine      = "goroutine"
	ProfileKindHeap           = "heap"
	ProfileKindMutex          = "mutex"
	ProfileDataFormatSVG      = "svg"
	ProfileDataFormatText     = "text"
	ProfileDataFormatProtobuf = "protobuf"
	ProfileDataFormatJeprof   = "jeprof"
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
	Target    ProfileTarget `json:"target"`
	ErrorList []string      `json:"-"`
	TsList    []int64       `json:"timestamp_list"`
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

type StatusCounter struct {
	finishedCount int
	runningCount  int
	failedCount   int
	totalCount    int
}

func (s *StatusCounter) AddStatus(status ProfileStatus) {
	s.totalCount++
	switch status {
	case ProfileStatusFinished:
		s.finishedCount++
	case ProfileStatusFailed:
		s.failedCount++
	case ProfileStatusRunning:
		s.runningCount++
	}
}

func (s *StatusCounter) GetFinalStatus() ProfileStatus {
	if s.finishedCount == s.totalCount {
		return ProfileStatusFinished
	}
	if s.failedCount == s.totalCount {
		return ProfileStatusFailed
	}
	if s.runningCount > 0 {
		return ProfileStatusRunning
	}
	return ProfileStatusFinishedWithError
}
