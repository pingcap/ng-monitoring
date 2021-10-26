package http

import (
	"archive/zip"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/log"
	"github.com/zhongzc/ng_monitoring/component/conprof"
	"github.com/zhongzc/ng_monitoring/component/conprof/meta"
	"github.com/zhongzc/ng_monitoring/component/topology"
	"github.com/zhongzc/ng_monitoring/config"
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
	c.Writer.Write(result)
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

func handleEstimateSize(c *gin.Context) {
	days := 0
	if value := c.Request.FormValue("days"); len(value) > 0 {
		v, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"status":  "error",
				"message": "params days value is invalid, should be int",
			})
			return
		}
		days = int(v)
	}
	if days == 0 {
		c.JSON(http.StatusOK, 0)
		return
	}
	_, suites := conprof.GetManager().GetAllCurrentScrapeSuite()
	totalSize := 0
	for _, suite := range suites {
		size := suite.LastScrapeSize()
		if size == 0 {
			size = 1024 * 1024
		}
		totalSize += size * 2
	}
	cfg := config.GetGlobalConfig().ContinueProfiling
	compressRatio := 10
	estimateSize := (days * 24 * 60 * 60 / cfg.IntervalSeconds) * totalSize / compressRatio
	c.JSON(http.StatusOK, estimateSize)
}

var (
	QueryStateSuccess       string = "success"
	QueryStateFailed        string = "failed"
	QueryStatePartialFailed string = "partial failed"
)

type ComponentNum struct {
	TiDB    int `json:"tidb"`
	PD      int `json:"pd"`
	TiKV    int `json:"tikv"`
	TiFlash int `json:"tiflash"`
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
	param, err := getBeginAndEndTimeParam(c.Request)
	if err != nil {
		return nil, err
	}

	profileLists, err := conprof.GetStorage().QueryGroupProfiles(param)
	if err != nil {
		return nil, err
	}
	m := make(map[int64]map[Target]struct{})
	for _, plist := range profileLists {
		target := Target{
			Component: plist.Target.Component,
			Address:   plist.Target.Address,
		}
		for _, ts := range plist.TsList {
			targets, ok := m[ts]
			if !ok {
				targets = make(map[Target]struct{})
				m[ts] = targets
			}
			targets[target] = struct{}{}
		}
	}
	groupProfiles := make([]GroupProfiles, 0, len(m))
	for ts, targets := range m {
		compMap := map[string]int{}
		for target := range targets {
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
			}
		}
		groupProfiles = append(groupProfiles, GroupProfiles{
			Ts:          ts,
			ProfileSecs: config.GetGlobalConfig().ContinueProfiling.ProfileSeconds, // todo: fix me
			State:       QueryStateSuccess,
			CompNum:     compNum,
		})
	}
	sort.Slice(groupProfiles, func(i, j int) bool {
		return groupProfiles[i].Ts > groupProfiles[j].Ts
	})
	return groupProfiles, nil
}

func queryGroupProfileDetail(c *gin.Context) (*GroupProfileDetail, error) {
	param, err := getTsParam(c.Request)
	if err != nil {
		return nil, err
	}

	profileLists, err := conprof.GetStorage().QueryGroupProfiles(param)
	if err != nil {
		return nil, err
	}

	targetProfiles := make([]ProfileDetail, 0, len(profileLists))
	for _, plist := range profileLists {
		targetProfiles = append(targetProfiles, ProfileDetail{
			State: QueryStateSuccess,
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
		State:          QueryStateSuccess,
		TargetProfiles: targetProfiles,
	}, nil
}

func querySingleProfileView(c *gin.Context) ([]byte, error) {
	param, err := getTsAndTargetParam(c.Request)
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
	if svg, err := ConvertToSVG(profileData); err == nil {
		return svg, nil
	}
	return profileData, nil
}

func queryAndDownload(c *gin.Context) error {
	param, err := getTsParam(c.Request)
	if err != nil {
		return err
	}

	c.Writer.Header().
		Set("Content-Disposition",
			fmt.Sprintf(`attachment; filename="profile"`+time.Unix(param.Begin, 0).Format("2006-01-02_15-04-05")+".zip"))
	zw := zip.NewWriter(c.Writer)
	fn := func(pt meta.ProfileTarget, ts int64, data []byte) error {
		fileName := fmt.Sprintf("%v_%v_%v_%v", pt.Kind, pt.Component, pt.Address, ts)
		svg, err := ConvertToSVG(data)
		if err == nil {
			data = svg
			fileName += ".svg"
		}
		fw, err := zw.Create(fileName)
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
	err = zw.Close()
	if err != nil {
		log.Error("handle download request failed", zap.Error(err))
	}
	return nil
}

var (
	beginTimeParamStr = "begin_time"
	endTimeParamStr   = "end_time"
	tsParamStr        = "ts"
)

func getBeginAndEndTimeParam(r *http.Request) (*meta.BasicQueryParam, error) {
	queryParam := &meta.BasicQueryParam{}
	params := []string{beginTimeParamStr, endTimeParamStr}
	for _, field := range params {
		v, ok, err := parseIntParamFromRequest(r, field)
		if err != nil {
			return nil, fmt.Errorf("invalid param %v value, error: %v", field, err)
		}
		if !ok {
			return nil, fmt.Errorf("need param %v", field)
		}
		switch field {
		case beginTimeParamStr:
			queryParam.Begin = v
		case endTimeParamStr:
			queryParam.End = v
		}
	}
	return queryParam, nil
}

func getTsParam(r *http.Request) (*meta.BasicQueryParam, error) {
	v, ok, err := parseIntParamFromRequest(r, tsParamStr)
	if err != nil {
		return nil, fmt.Errorf("invalid param %v value, error: %v", tsParamStr, err)
	}
	if !ok {
		return nil, fmt.Errorf("need param %v", tsParamStr)
	}
	queryParam := &meta.BasicQueryParam{
		Begin: v,
		End:   v,
	}
	return queryParam, nil
}

func getTsAndTargetParam(r *http.Request) (*meta.BasicQueryParam, error) {
	queryParam, err := getTsParam(r)
	if err != nil {
		return nil, err
	}
	params := []string{"profile_type", "component", "address"}
	values := make([]string, len(params))
	for i, param := range params {
		if v := r.FormValue(param); len(v) > 0 {
			values[i] = v
		} else {
			return nil, fmt.Errorf("need param %v", param)
		}
	}
	queryParam.Targets = append(queryParam.Targets, meta.ProfileTarget{
		Kind:      values[0],
		Component: values[1],
		Address:   values[2],
	})
	return queryParam, nil
}

func parseIntParamFromRequest(r *http.Request, param string) (value int64, ok bool, err error) {
	if v := r.FormValue(param); len(v) > 0 {
		value, err = strconv.ParseInt(v, 10, 64)
		if err != nil {
			return value, true, err
		}
		return value, true, nil
	}
	return value, false, nil
}
