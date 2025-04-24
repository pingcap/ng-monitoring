package http

import (
	"archive/zip"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/ng-monitoring/component/conprof"
	"github.com/pingcap/ng-monitoring/component/conprof/jeprof"
	"github.com/pingcap/ng-monitoring/component/conprof/meta"
	"github.com/pingcap/ng-monitoring/component/topology"
	"github.com/pingcap/ng-monitoring/config"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

func HTTPService(g *gin.RouterGroup) {
	g.GET("/group_profiles", handleGroupProfiles)
	g.GET("/group_profile/detail", handleGroupProfileDetail)
	g.GET("/single_profile/view", handleSingleProfileView)
	g.GET("/download", handleDownload)
	g.GET("/components", handleComponents)
	g.GET("/estimate_size", handleEstimateSize)
}

func handleGroupProfiles(c *gin.Context) {
	result, err := queryGroupProfiles(c)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status":  "error",
			"message": err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, result)
}

func handleGroupProfileDetail(c *gin.Context) {
	result, err := queryGroupProfileDetail(c)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status":  "error",
			"message": err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, result)
}

func handleSingleProfileView(c *gin.Context) {
	result, err := querySingleProfileView(c)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status":  "error",
			"message": err.Error(),
		})
		return
	}
	c.Writer.WriteHeader(http.StatusOK)
	_, _ = c.Writer.Write(result)
}

func handleDownload(c *gin.Context) {
	err := queryAndDownload(c)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status":  "error",
			"message": err.Error(),
		})
		return
	}
}

func handleComponents(c *gin.Context) {
	components := conprof.GetManager().GetCurrentScrapeComponents()
	c.JSON(http.StatusOK, components)
}

type EstimateSize struct {
	InstanceCount int `json:"instance_count"`
	ProfileSize   int `json:"profile_size"`
}

func handleEstimateSize(c *gin.Context) {
	components := topology.GetCurrentComponent()
	totalSize := 0
	for _, comp := range components {
		size := getProfileEstimateSize(comp)
		totalSize += size
	}
	cfg := config.GetGlobalConfig().ContinueProfiling
	estimateSize := (24 * 60 * 60 / cfg.IntervalSeconds) * totalSize
	c.JSON(http.StatusOK, EstimateSize{
		InstanceCount: len(components),
		ProfileSize:   estimateSize,
	})
}

var defaultProfileSize = 128 * 1024

func getProfileEstimateSize(component topology.Component) int {
	switch component.Name {
	case topology.ComponentPD:
		return 20*1024 + // profile size
			25*1024 + // goroutine size
			100*1024 + // heap size
			30*1024 // mutex size
	case topology.ComponentTiDB:
		return 100*1024 + // profile size
			100*1024 + // goroutine size
			400*1024 + // heap size
			30*1024 // mutex size
	case topology.ComponentTiCDC:
		return 100*1024 + // profile size
			100*1024 + // goroutine size
			400*1024 + // heap size
			30*1024 // mutex size
	case topology.ComponentTiKV:
		return 200*1024 + // profile size
			200*1024 // heap size
	case topology.ComponentTiFlash:
		// TODO: remove this after TiFlash fix the profile bug.
		return 0
	}
	return defaultProfileSize
}

type ComponentNum struct {
	TiDB    int `json:"tidb"`
	PD      int `json:"pd"`
	TiKV    int `json:"tikv"`
	TiFlash int `json:"tiflash"`
	TiCDC   int `json:"ticdc"`
}

type GroupProfiles struct {
	Ts          int64        `json:"ts"`
	ProfileSecs int          `json:"profile_duration_secs"`
	State       string       `json:"state"`
	CompNum     ComponentNum `json:"component_num"`
}

type GroupProfileDetail struct {
	Ts             int64           `json:"ts"`
	ProfileSecs    int             `json:"profile_duration_secs"`
	State          string          `json:"state"`
	TargetProfiles []ProfileDetail `json:"target_profiles"`
}

type ProfileDetail struct {
	State  string `json:"state"`
	Error  string `json:"error"`
	Type   string `json:"profile_type"`
	Target Target `json:"target"`
}

type Target struct {
	Component string `json:"component"`
	Address   string `json:"address"`
}

func queryGroupProfiles(c *gin.Context) ([]GroupProfiles, error) {
	param, err := buildQueryParam(c.Request, []string{beginTimeParamStr, endTimeParamStr}, []string{limitParamStr})
	if err != nil {
		return nil, err
	}

	profileLists, err := conprof.GetStorage().QueryGroupProfiles(param)
	if err != nil {
		return nil, err
	}
	m := make(map[int64]*StatusCounterAndTargets)
	for _, plist := range profileLists {
		target := Target{
			Component: plist.Target.Component,
			Address:   plist.Target.Address,
		}
		for idx, ts := range plist.TsList {
			sc, ok := m[ts]
			if !ok {
				sc = &StatusCounterAndTargets{targets: make(map[Target]struct{})}
				m[ts] = sc
			}
			status := getStateFromError(plist.ErrorList[idx])
			sc.AddStatus(status)
			sc.targets[target] = struct{}{}
		}
	}
	groupProfiles := make([]GroupProfiles, 0, len(m))
	lastTS := conprof.GetManager().GetLastScrapeTime().Unix()
	for ts, sc := range m {
		var status meta.ProfileStatus
		if ts == lastTS {
			status = conprof.GetManager().GetRunningStatus()
		} else {
			status = sc.GetFinalStatus()
		}
		compNum := sc.getComponentNum()
		groupProfiles = append(groupProfiles, GroupProfiles{
			Ts:          ts,
			ProfileSecs: config.GetGlobalConfig().ContinueProfiling.ProfileSeconds, // todo: fix me
			State:       status.String(),
			CompNum:     compNum,
		})
	}
	sort.Slice(groupProfiles, func(i, j int) bool {
		return groupProfiles[i].Ts > groupProfiles[j].Ts
	})
	return groupProfiles, nil
}

type StatusCounterAndTargets struct {
	meta.StatusCounter
	targets map[Target]struct{}
}

func (sc *StatusCounterAndTargets) getComponentNum() ComponentNum {
	compMap := map[string]int{}
	for target := range sc.targets {
		compMap[target.Component] += 1
	}
	compNum := ComponentNum{}
	for comp, num := range compMap {
		switch comp {
		case topology.ComponentTiDB:
			compNum.TiDB = num
		case topology.ComponentPD:
			compNum.PD = num
		case topology.ComponentTiKV:
			compNum.TiKV = num
		case topology.ComponentTiFlash:
			compNum.TiFlash = num
		case topology.ComponentTiCDC:
			compNum.TiCDC = num
		}
	}
	return compNum
}

func queryGroupProfileDetail(c *gin.Context) (*GroupProfileDetail, error) {
	param, err := buildQueryParam(c.Request, []string{tsParamStr}, []string{limitParamStr})
	if err != nil {
		return nil, err
	}

	profileLists, err := conprof.GetStorage().QueryGroupProfiles(param)
	if err != nil {
		return nil, err
	}

	targetProfiles := make([]ProfileDetail, 0, len(profileLists))
	statusCounter := meta.StatusCounter{}
	for _, plist := range profileLists {
		if len(plist.ErrorList) == 0 {
			continue
		}
		profileError := plist.ErrorList[0]
		status := getStateFromError(profileError)
		statusCounter.AddStatus(status)
		targetProfiles = append(targetProfiles, ProfileDetail{
			State: status.String(),
			Error: profileError,
			Type:  plist.Target.Kind,
			Target: Target{
				Component: plist.Target.Component,
				Address:   plist.Target.Address,
			},
		})
	}
	sort.Slice(targetProfiles, func(i, j int) bool {
		return targetProfiles[i].Target.Address < targetProfiles[j].Target.Address
	})
	return &GroupProfileDetail{
		Ts:             param.Begin,
		ProfileSecs:    config.GetGlobalConfig().ContinueProfiling.ProfileSeconds,
		State:          statusCounter.GetFinalStatus().String(),
		TargetProfiles: targetProfiles,
	}, nil
}

func getStateFromError(err string) meta.ProfileStatus {
	if err == "" {
		return meta.ProfileStatusFinished
	}
	return meta.ProfileStatusFailed
}

func querySingleProfileView(c *gin.Context) ([]byte, error) {
	param, err := buildQueryParam(c.Request, []string{tsParamStr}, []string{limitParamStr, dataFormatParamStr})
	if err != nil {
		return nil, err
	}
	err = getTargetFromRequest(c.Request, param, true)
	if err != nil {
		return nil, err
	}

	var profileData []byte
	err = conprof.GetStorage().QueryProfileData(param, func(target meta.ProfileTarget, ts int64, data []byte) error {
		profileData = data
		return nil
	})
	if err != nil {
		return nil, err
	}
	if param.Targets[0].Component == topology.ComponentTiKV && param.Targets[0].Kind == meta.ProfileKindHeap {
		if param.DataFormat == meta.ProfileDataFormatSVG {
			return jeprof.ConvertToSVG(profileData)
		} else if param.DataFormat == meta.ProfileDataFormatText {
			return jeprof.ConvertToText(profileData)
		}
	} else if param.DataFormat == meta.ProfileDataFormatSVG {
		if svg, err := ConvertToSVG(profileData); err == nil {
			return svg, nil
		}
	}
	return profileData, nil
}

func queryAndDownload(c *gin.Context) error {
	var param *meta.BasicQueryParam
	var err error
	if v := c.Request.FormValue(beginTimeParamStr); len(v) > 0 {
		param, err = buildQueryParam(c.Request, []string{beginTimeParamStr, endTimeParamStr}, []string{limitParamStr})
	} else {
		param, err = buildQueryParam(c.Request, []string{tsParamStr}, []string{limitParamStr})
	}
	if err != nil {
		return err
	}

	err = getTargetFromRequest(c.Request, param, false)
	if err != nil {
		return err
	}

	c.Writer.Header().
		Set("Content-Disposition", `attachment; filename="profile"`+time.Unix(param.Begin, 0).Format("2006-01-02_15-04-05")+".zip")
	zw := zip.NewWriter(c.Writer)
	fn := func(pt meta.ProfileTarget, ts int64, data []byte) error {
		fileName := fmt.Sprintf("%v_%v_%v_%v", pt.Kind, pt.Component, pt.Address, ts)
		fileName = strings.ReplaceAll(fileName, ":", "_")
		if pt.Kind == meta.ProfileKindGoroutine {
			fileName += ".txt"
		} else if pt.Kind == meta.ProfileKindHeap && pt.Component == topology.ComponentTiKV {
			fileName += ".prof"
		} else {
			fileName += ".proto"
		}
		fw, err := zw.CreateHeader(&zip.FileHeader{
			Name:     fileName,
			Method:   zip.Deflate,
			Modified: time.Now(),
		})
		if err != nil {
			return err
		}
		_, err = fw.Write(data)
		return err
	}

	err = conprof.GetStorage().QueryProfileData(param, fn)
	if err != nil {
		return err
	}
	fw, err := zw.CreateHeader(&zip.FileHeader{
		Name:     "README.md",
		Method:   zip.Deflate,
		Modified: time.Now(),
	})
	if err != nil {
		return err
	}
	_, err = fw.Write([]byte(downloadReadme))
	if err != nil {
		return err
	}
	err = zw.Close()
	if err != nil {
		log.Error("handle download request failed", zap.Error(err))
	}
	return nil
}

const downloadReadme = `
To review the go profile data whose file name suffix is '.proto' interactively:
$ go tool pprof --http=127.0.0.1:6060 profile_xxx.proto

To review the jemalloc profile data whose file name suffix is '.prof' interactively:
$ jeprof --web profile_xxx.prof
`

var (
	beginTimeParamStr   = "begin_time"
	endTimeParamStr     = "end_time"
	tsParamStr          = "ts"
	limitParamStr       = "limit"
	dataFormatParamStr  = "data_format"
	defdataFormatParam  = meta.ProfileDataFormatSVG
	profileTypeParamStr = "profile_type"
	componentParamStr   = "component"
	addressParamStr     = "address"
)

func buildQueryParam(r *http.Request, requires []string, options []string) (*meta.BasicQueryParam, error) {
	param := &meta.BasicQueryParam{}
	for _, paramName := range requires {
		err := getParamFromRequest(r, param, paramName, true)
		if err != nil {
			return nil, err
		}
	}
	for _, paramName := range options {
		err := getParamFromRequest(r, param, paramName, false)
		if err != nil {
			return nil, err
		}
	}

	// set default value
	if param.DataFormat == "" {
		param.DataFormat = defdataFormatParam
	}
	return param, nil
}

func getParamFromRequest(r *http.Request, param *meta.BasicQueryParam, paramName string, isRequired bool) error {
	v := r.FormValue(paramName)
	if len(v) == 0 {
		if isRequired {
			return fmt.Errorf("need param %v", paramName)
		}
		return nil
	}
	switch paramName {
	case tsParamStr:
		value, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid param %v value, error: %v", paramName, err)
		}
		param.Begin, param.End = value, value
	case beginTimeParamStr:
		value, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid param %v value, error: %v", paramName, err)
		}
		param.Begin = value
	case endTimeParamStr:
		value, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid param %v value, error: %v", paramName, err)
		}
		param.End = value
	case limitParamStr:
		value, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid param %v value, error: %v", paramName, err)
		}
		param.Limit = value
	case dataFormatParamStr:
		switch v {
		case meta.ProfileDataFormatSVG, meta.ProfileDataFormatProtobuf, meta.ProfileDataFormatJeprof, meta.ProfileDataFormatText:
			param.DataFormat = v
		default:
			return fmt.Errorf("invalid param %v value %v, expected: %v, %v, %v, %v",
				dataFormatParamStr, v, meta.ProfileDataFormatSVG, meta.ProfileDataFormatProtobuf, meta.ProfileDataFormatJeprof, meta.ProfileDataFormatText)
		}
	default:
		return fmt.Errorf("unknow param %s", paramName)
	}
	return nil
}

func getTargetFromRequest(r *http.Request, param *meta.BasicQueryParam, isRequired bool) error {
	paramNames := []string{profileTypeParamStr, componentParamStr, addressParamStr}
	values := make([]string, len(paramNames))
	for i, paramName := range paramNames {
		if v := r.FormValue(paramName); len(v) > 0 {
			values[i] = v
		} else {
			if isRequired {
				return fmt.Errorf("need param %v", paramName)
			}
			return nil
		}
	}
	param.Targets = append(param.Targets, meta.ProfileTarget{
		Kind:      values[0],
		Component: values[1],
		Address:   values[2],
	})
	return nil
}
