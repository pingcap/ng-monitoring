package tests

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/promql"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/genjidb/genji"
	"github.com/gin-gonic/gin"
	"github.com/golang/protobuf/proto"
	rua "github.com/pingcap/kvproto/pkg/resource_usage_agent"
	"github.com/pingcap/ng-monitoring/component/topology"
	"github.com/pingcap/ng-monitoring/component/topsql"
	"github.com/pingcap/ng-monitoring/component/topsql/query"
	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/ng-monitoring/config/pdvariable"
	"github.com/pingcap/ng-monitoring/database/timeseries"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/suite"
)

func TestTopSQL(t *testing.T) {
	suite.Run(t, &testTopSQLSuite{})
}

const testTsdbPath = "/tmp/ng-monitoring-test/tsdb"

var testBaseTs = uint64(time.Now().Unix()) - 60*60*24

type baseHttpResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

type instancesHttpResponse struct {
	baseHttpResponse
	Data []query.InstanceItem `json:"data"`
}

type metricsHttpResponse struct {
	baseHttpResponse
	Data []query.TopSQLItem `json:"data"`
}

type testTopSQLSuite struct {
	suite.Suite
	cfg        *config.Config
	db         *genji.DB
	tidbAddr   string
	tikvAddr   string
	tidbServer *MockTiDBServer
	tikvServer *MockTiKVServer
	topCh      topology.Subscriber
	varCh      pdvariable.Subscriber
	cfgCh      config.Subscriber
	ng         *gin.Engine
}

func (s *testTopSQLSuite) SetupSuite() {
	// init config
	config.StoreGlobalConfig(&config.Config{
		Address:           "",
		AdvertiseAddress:  "",
		PD:                config.PD{},
		Log:               config.Log{},
		Storage:           config.Storage{},
		ContinueProfiling: config.ContinueProfilingConfig{},
		Security:          config.Security{},
	})

	// init local mock tidb server
	s.tidbServer = NewMockTiDBServer()
	tidbAddr, err := s.tidbServer.Listen()
	s.tidbAddr = tidbAddr
	arr := strings.Split(tidbAddr, ":")
	tidbTestIp := arr[0]
	tidbTestPort, err := strconv.Atoi(arr[1])
	s.NoError(err)
	go func() {
		s.NoError(s.tidbServer.Serve())
	}()

	// init local mock tikv server
	s.tikvServer = NewMockTiKVServer()
	tikvAddr, err := s.tikvServer.Listen()
	s.NoError(err)
	s.tikvAddr = tikvAddr
	arr = strings.Split(tikvAddr, ":")
	tikvTestIp := arr[0]
	tikvTestPort, err := strconv.Atoi(arr[1])
	s.NoError(err)
	go func() {
		s.NoError(s.tikvServer.Serve())
	}()
	time.Sleep(time.Second) // wait for grpc server ready (ugly code)

	// init genji in memory
	s.db, err = genji.Open(":memory:")
	s.NoError(err)

	// init tsdb logger
	_ = flag.Set("loggerOutput", "stderr")
	_ = flag.Set("loggerLevel", "WARN")
	logger.Init()

	// init vm
	s.NoError(os.RemoveAll(testTsdbPath))
	_ = flag.Set("storageDataPath", testTsdbPath)
	_ = flag.Set("retentionPeriod", "1")
	storage.SetMinScrapeIntervalForDeduplication(0)
	vmstorage.Init(promql.ResetRollupResultCacheIfNeeded)
	vmselect.Init()
	vminsert.Init()

	// init topsql
	cfg := config.GetDefaultConfig()
	s.cfg = &cfg
	s.topCh = make(topology.Subscriber)
	s.varCh = make(pdvariable.Subscriber)
	s.cfgCh = make(config.Subscriber)
	err = topsql.Init(s.cfg, s.cfgCh, s.db, timeseries.InsertHandler, timeseries.SelectHandler, s.topCh, s.varCh)
	s.NoError(err)
	s.varCh <- &pdvariable.PDVariable{EnableTopSQL: true}
	time.Sleep(100 * time.Millisecond)
	s.topCh <- []topology.Component{{
		Name:       topology.ComponentTiDB,
		IP:         tidbTestIp,
		Port:       uint(tidbTestPort),
		StatusPort: uint(tidbTestPort),
	}, {
		Name:       topology.ComponentTiKV,
		IP:         tikvTestIp,
		Port:       uint(tikvTestPort),
		StatusPort: uint(tikvTestPort),
	}}
	time.Sleep(100 * time.Millisecond)

	ng := gin.New()
	topsql.HTTPService(ng.Group(""))
	s.ng = ng

	// init data
	s.tidbServer.PushRecords([]tipb.TopSQLRecord{{
		SqlDigest:                   []byte("sql-1"),
		PlanDigest:                  []byte("plan-1"),
		RecordListTimestampSec:      []uint64{testBaseTs + 111, testBaseTs + 112, testBaseTs + 113, testBaseTs + 114, testBaseTs + 115},
		RecordListCpuTimeMs:         []uint32{121, 122, 123, 124, 125},
		RecordListStmtExecCount:     []uint64{131, 132, 133, 134, 135},
		RecordListStmtDurationSumNs: []uint64{141, 142, 143, 144, 145},
		RecordListStmtKvExecCount: []*tipb.TopSQLStmtKvExecCount{
			{ExecCount: map[string]uint64{"tikv-1": 151, "tikv-2": 251}},
			{ExecCount: map[string]uint64{"tikv-1": 152, "tikv-2": 252}},
			{ExecCount: map[string]uint64{"tikv-1": 153, "tikv-2": 253}},
			{ExecCount: map[string]uint64{"tikv-1": 154, "tikv-2": 254}},
			{ExecCount: map[string]uint64{"tikv-1": 155, "tikv-2": 255}},
		},
	}})
	s.tikvServer.PushRecords([]*rua.ResourceUsageRecord{{
		ResourceGroupTag:       s.encodeTag([]byte("sql-1"), []byte("plan-1"), tipb.ResourceGroupTagLabel_ResourceGroupTagLabelRow),
		RecordListTimestampSec: []uint64{testBaseTs + 111, testBaseTs + 112, testBaseTs + 113, testBaseTs + 114, testBaseTs + 115},
		RecordListCpuTimeMs:    []uint32{121, 122, 123, 124, 125},
		RecordListReadKeys:     []uint32{131, 132, 133, 134, 135},
		RecordListWriteKeys:    []uint32{141, 142, 143, 144, 145},
	}, {
		ResourceGroupTag:       s.encodeTag([]byte("sql-2"), []byte("plan-2"), tipb.ResourceGroupTagLabel_ResourceGroupTagLabelIndex),
		RecordListTimestampSec: []uint64{testBaseTs + 211, testBaseTs + 212, testBaseTs + 213, testBaseTs + 214, testBaseTs + 215},
		RecordListCpuTimeMs:    []uint32{221, 222, 223, 224, 225},
		RecordListReadKeys:     []uint32{231, 232, 233, 234, 235},
		RecordListWriteKeys:    []uint32{241, 242, 243, 244, 245},
	}, {
		// Unknown label will not be counted in the read_row/read_index/write_row/write_index.
		ResourceGroupTag:       s.encodeTag([]byte("sql-3"), []byte("plan-3"), tipb.ResourceGroupTagLabel_ResourceGroupTagLabelUnknown),
		RecordListTimestampSec: []uint64{testBaseTs + 311, testBaseTs + 312, testBaseTs + 313, testBaseTs + 314, testBaseTs + 315},
		RecordListCpuTimeMs:    []uint32{321, 322, 323, 324, 325},
		RecordListReadKeys:     []uint32{331, 332, 333, 334, 335},
		RecordListWriteKeys:    []uint32{341, 342, 343, 344, 345},
	}})
	time.Sleep(3 * time.Second)
}

func (s *testTopSQLSuite) TearDownSuite() {
	s.tikvServer.Stop()
	topsql.Stop()
	vminsert.Stop()
	vmselect.Stop()
	vmstorage.Stop()
	s.NoError(os.RemoveAll(testTsdbPath))
}

func (s *testTopSQLSuite) TestInstances() {
	w := NewMockResponseWriter()
	req, err := http.NewRequest(http.MethodGet, "/v1/instances", nil)
	s.NoError(err)
	s.ng.ServeHTTP(w, req)
	if w.StatusCode != http.StatusOK {
		s.FailNow(fmt.Sprintf("http: %d, body: %s\n", w.StatusCode, string(w.Body)))
	}
	resp := instancesHttpResponse{}
	s.NoError(json.Unmarshal(w.Body, &resp))
	if resp.Status != "ok" {
		s.FailNow(fmt.Sprintf("status: %s, message: %s, body: %v\n", resp.Status, resp.Message, string(w.Body)))
	}
	s.Len(resp.Data, 2)
	for _, item := range resp.Data {
		switch item.InstanceType {
		case topology.ComponentTiDB:
			s.Equal(s.tidbAddr, item.Instance)
		case topology.ComponentTiKV:
			s.Equal(s.tikvAddr, item.Instance)
		default:
			panic("unknown component type: " + item.InstanceType)
		}
	}
}

func (s *testTopSQLSuite) TestCpuTime() {
	s.testCpuTime(int(testBaseTs+111), 121)
	s.testCpuTime(int(testBaseTs+211), 221)
	s.testCpuTime(int(testBaseTs+311), 321)
}

func (s *testTopSQLSuite) TestReadRow() {
	s.testReadRow(int(testBaseTs+111), 131) // ResourceGroupTagLabelRow
	s.testReadRow(int(testBaseTs+211), 0)   // ResourceGroupTagLabelIndex
	s.testReadRow(int(testBaseTs+311), 0)   // ResourceGroupTagLabelUnknown
}

func (s *testTopSQLSuite) TestReadIndex() {
	s.testReadIndex(int(testBaseTs+111), 0)   // ResourceGroupTagLabelRow
	s.testReadIndex(int(testBaseTs+211), 231) // ResourceGroupTagLabelIndex
	s.testReadIndex(int(testBaseTs+311), 0)   // ResourceGroupTagLabelUnknown
}

func (s *testTopSQLSuite) TestWriteRow() {
	s.testWriteRow(int(testBaseTs+111), 141) // ResourceGroupTagLabelRow
	s.testWriteRow(int(testBaseTs+211), 0)   // ResourceGroupTagLabelIndex
	s.testWriteRow(int(testBaseTs+311), 0)   // ResourceGroupTagLabelUnknown
}

func (s *testTopSQLSuite) TestWriteIndex() {
	s.testWriteIndex(int(testBaseTs+111), 0)   // ResourceGroupTagLabelRow
	s.testWriteIndex(int(testBaseTs+211), 241) // ResourceGroupTagLabelIndex
	s.testWriteIndex(int(testBaseTs+311), 0)   // ResourceGroupTagLabelUnknown
}

func (s *testTopSQLSuite) TestSQLExecCount() {
	s.testSQLExecCount(s.tidbAddr, int(testBaseTs+111), 131)
	s.testSQLExecCount("tikv-1", int(testBaseTs+111), 151)
	s.testSQLExecCount("tikv-2", int(testBaseTs+111), 251)
}

func (s *testTopSQLSuite) TestSQLDurationSum() {
	s.testSQLDurationSum(int(testBaseTs+111), 141)
}

func (s *testTopSQLSuite) testCpuTime(baseTs int, baseValue int) {
	w := NewMockResponseWriter()
	req, err := http.NewRequest(http.MethodGet, "/v1/cpu_time", nil)
	s.NoError(err)
	urlQuery := url.Values{}
	urlQuery.Set("instance", s.tikvAddr)
	urlQuery.Set("start", strconv.Itoa(baseTs))
	urlQuery.Set("end", strconv.Itoa(baseTs+5))
	urlQuery.Set("window", "1s")
	req.URL.RawQuery = urlQuery.Encode()
	s.ng.ServeHTTP(w, req)
	if w.StatusCode != http.StatusOK {
		s.FailNow(fmt.Sprintf("http: %d, body: %s\n", w.StatusCode, string(w.Body)))
	}
	resp := metricsHttpResponse{}
	s.NoError(json.Unmarshal(w.Body, &resp))
	if resp.Status != "ok" {
		s.FailNow(fmt.Sprintf("status: %s, message: %s, body: %v\n", resp.Status, resp.Message, string(w.Body)))
	}
	s.Len(resp.Data, 1)
	s.Len(resp.Data[0].Plans, 1)
	s.Len(resp.Data[0].Plans[0].TimestampSecs, 5)
	s.Len(resp.Data[0].Plans[0].CPUTimeMillis, 5)
	s.Empty(resp.Data[0].Plans[0].ReadRows)
	s.Empty(resp.Data[0].Plans[0].ReadIndexes)
	s.Empty(resp.Data[0].Plans[0].WriteRows)
	s.Empty(resp.Data[0].Plans[0].WriteIndexes)
	for n := 0; n < 5; n++ {
		s.Equal(uint64(baseTs+n), resp.Data[0].Plans[0].TimestampSecs[n])
		s.Equal(uint64(baseValue+n), resp.Data[0].Plans[0].CPUTimeMillis[n])
	}
}

func (s *testTopSQLSuite) testReadRow(baseTs int, baseValue int) {
	w := NewMockResponseWriter()
	req, err := http.NewRequest(http.MethodGet, "/v1/read_row", nil)
	s.NoError(err)
	urlQuery := url.Values{}
	urlQuery.Set("instance", s.tikvAddr)
	urlQuery.Set("start", strconv.Itoa(baseTs))
	urlQuery.Set("end", strconv.Itoa(baseTs+5))
	urlQuery.Set("window", "1s")
	req.URL.RawQuery = urlQuery.Encode()
	s.ng.ServeHTTP(w, req)
	if w.StatusCode != http.StatusOK {
		s.FailNow(fmt.Sprintf("http: %d, body: %s\n", w.StatusCode, string(w.Body)))
	}
	resp := metricsHttpResponse{}
	s.NoError(json.Unmarshal(w.Body, &resp))
	if resp.Status != "ok" {
		s.FailNow(fmt.Sprintf("status: %s, message: %s, body: %v\n", resp.Status, resp.Message, string(w.Body)))
	}
	s.Len(resp.Data, 1)
	s.Len(resp.Data[0].Plans, 1)
	s.Len(resp.Data[0].Plans[0].TimestampSecs, 5)
	s.Len(resp.Data[0].Plans[0].ReadRows, 5)
	s.Empty(resp.Data[0].Plans[0].CPUTimeMillis)
	s.Empty(resp.Data[0].Plans[0].ReadIndexes)
	s.Empty(resp.Data[0].Plans[0].WriteRows)
	s.Empty(resp.Data[0].Plans[0].WriteIndexes)
	for n := 0; n < 5; n++ {
		s.Equal(uint64(baseTs+n), resp.Data[0].Plans[0].TimestampSecs[n])
		if baseValue == 0 {
			s.Equal(uint64(0), resp.Data[0].Plans[0].ReadRows[n])
		} else {
			s.Equal(uint64(baseValue+n), resp.Data[0].Plans[0].ReadRows[n])
		}
	}
}

func (s *testTopSQLSuite) testReadIndex(baseTs int, baseValue int) {
	w := NewMockResponseWriter()
	req, err := http.NewRequest(http.MethodGet, "/v1/read_index", nil)
	s.NoError(err)
	urlQuery := url.Values{}
	urlQuery.Set("instance", s.tikvAddr)
	urlQuery.Set("start", strconv.Itoa(baseTs))
	urlQuery.Set("end", strconv.Itoa(baseTs+5))
	urlQuery.Set("window", "1s")
	req.URL.RawQuery = urlQuery.Encode()
	s.ng.ServeHTTP(w, req)
	if w.StatusCode != http.StatusOK {
		s.FailNow(fmt.Sprintf("http: %d, body: %s\n", w.StatusCode, string(w.Body)))
	}
	resp := metricsHttpResponse{}
	s.NoError(json.Unmarshal(w.Body, &resp))
	if resp.Status != "ok" {
		s.FailNow(fmt.Sprintf("status: %s, message: %s, body: %v\n", resp.Status, resp.Message, string(w.Body)))
	}
	s.Len(resp.Data, 1)
	s.Len(resp.Data[0].Plans, 1)
	s.Len(resp.Data[0].Plans[0].TimestampSecs, 5)
	s.Len(resp.Data[0].Plans[0].ReadIndexes, 5)
	s.Empty(resp.Data[0].Plans[0].CPUTimeMillis)
	s.Empty(resp.Data[0].Plans[0].ReadRows)
	s.Empty(resp.Data[0].Plans[0].WriteRows)
	s.Empty(resp.Data[0].Plans[0].WriteIndexes)
	for n := 0; n < 5; n++ {
		s.Equal(uint64(baseTs+n), resp.Data[0].Plans[0].TimestampSecs[n])
		if baseValue == 0 {
			s.Equal(uint64(0), resp.Data[0].Plans[0].ReadIndexes[n])
		} else {
			s.Equal(uint64(baseValue+n), resp.Data[0].Plans[0].ReadIndexes[n])
		}
	}
}

func (s *testTopSQLSuite) testWriteRow(baseTs int, baseValue int) {
	var err error
	w := NewMockResponseWriter()
	req, err := http.NewRequest(http.MethodGet, "/v1/write_row", nil)
	s.NoError(err)
	urlQuery := url.Values{}
	urlQuery.Set("instance", s.tikvAddr)
	urlQuery.Set("start", strconv.Itoa(baseTs))
	urlQuery.Set("end", strconv.Itoa(baseTs+5))
	urlQuery.Set("window", "1s")
	req.URL.RawQuery = urlQuery.Encode()
	s.ng.ServeHTTP(w, req)
	if w.StatusCode != http.StatusOK {
		s.FailNow(fmt.Sprintf("http: %d, body: %s\n", w.StatusCode, string(w.Body)))
	}
	resp := metricsHttpResponse{}
	s.NoError(json.Unmarshal(w.Body, &resp))
	if resp.Status != "ok" {
		s.FailNow(fmt.Sprintf("status: %s, message: %s, body: %v\n", resp.Status, resp.Message, string(w.Body)))
	}
	s.Len(resp.Data, 1)
	s.Len(resp.Data[0].Plans, 1)
	s.Len(resp.Data[0].Plans[0].TimestampSecs, 5)
	s.Len(resp.Data[0].Plans[0].WriteRows, 5)
	s.Empty(resp.Data[0].Plans[0].CPUTimeMillis)
	s.Empty(resp.Data[0].Plans[0].ReadRows)
	s.Empty(resp.Data[0].Plans[0].ReadIndexes)
	s.Empty(resp.Data[0].Plans[0].WriteIndexes)
	for n := 0; n < 5; n++ {
		s.Equal(uint64(baseTs+n), resp.Data[0].Plans[0].TimestampSecs[n])
		if baseValue == 0 {
			s.Equal(uint64(0), resp.Data[0].Plans[0].WriteRows[n])
		} else {
			s.Equal(uint64(baseValue+n), resp.Data[0].Plans[0].WriteRows[n])
		}
	}
}

func (s *testTopSQLSuite) testWriteIndex(baseTs int, baseValue int) {
	w := NewMockResponseWriter()
	req, err := http.NewRequest(http.MethodGet, "/v1/write_index", nil)
	s.NoError(err)
	urlQuery := url.Values{}
	urlQuery.Set("instance", s.tikvAddr)
	urlQuery.Set("start", strconv.Itoa(baseTs))
	urlQuery.Set("end", strconv.Itoa(baseTs+5))
	urlQuery.Set("window", "1s")
	req.URL.RawQuery = urlQuery.Encode()
	s.ng.ServeHTTP(w, req)
	if w.StatusCode != http.StatusOK {
		s.FailNow(fmt.Sprintf("http: %d, body: %s\n", w.StatusCode, string(w.Body)))
	}
	resp := metricsHttpResponse{}
	s.NoError(json.Unmarshal(w.Body, &resp))
	if resp.Status != "ok" {
		s.FailNow(fmt.Sprintf("status: %s, message: %s, body: %v\n", resp.Status, resp.Message, string(w.Body)))
	}
	s.Len(resp.Data, 1)
	s.Len(resp.Data[0].Plans, 1)
	s.Len(resp.Data[0].Plans[0].TimestampSecs, 5)
	s.Len(resp.Data[0].Plans[0].WriteIndexes, 5)
	s.Empty(resp.Data[0].Plans[0].CPUTimeMillis)
	s.Empty(resp.Data[0].Plans[0].ReadRows)
	s.Empty(resp.Data[0].Plans[0].ReadIndexes)
	s.Empty(resp.Data[0].Plans[0].WriteRows)
	for n := 0; n < 5; n++ {
		s.Equal(uint64(baseTs+n), resp.Data[0].Plans[0].TimestampSecs[n])
		if baseValue == 0 {
			s.Equal(uint64(0), resp.Data[0].Plans[0].WriteIndexes[n])
		} else {
			s.Equal(uint64(baseValue+n), resp.Data[0].Plans[0].WriteIndexes[n])
		}
	}
}

func (s *testTopSQLSuite) testSQLExecCount(target string, baseTs int, baseValue int) {
	w := NewMockResponseWriter()
	req, err := http.NewRequest(http.MethodGet, "/v1/sql_exec_count", nil)
	s.NoError(err)
	urlQuery := url.Values{}
	urlQuery.Set("instance", target)
	urlQuery.Set("start", strconv.Itoa(baseTs))
	urlQuery.Set("end", strconv.Itoa(baseTs+5))
	urlQuery.Set("window", "1s")
	req.URL.RawQuery = urlQuery.Encode()
	s.ng.ServeHTTP(w, req)
	if w.StatusCode != http.StatusOK {
		s.FailNow(fmt.Sprintf("http: %d, body: %s\n", w.StatusCode, string(w.Body)))
	}
	resp := metricsHttpResponse{}
	s.NoError(json.Unmarshal(w.Body, &resp))
	if resp.Status != "ok" {
		s.FailNow(fmt.Sprintf("status: %s, message: %s, body: %v\n", resp.Status, resp.Message, string(w.Body)))
	}
	s.Len(resp.Data, 1)
	s.Len(resp.Data[0].Plans, 1)
	s.Len(resp.Data[0].Plans[0].TimestampSecs, 5)
	s.Len(resp.Data[0].Plans[0].SQLExecCount, 5)
	for n := 0; n < 5; n++ {
		s.Equal(uint64(baseTs+n), resp.Data[0].Plans[0].TimestampSecs[n])
		s.Equal(uint64(baseValue+n), resp.Data[0].Plans[0].SQLExecCount[n])
	}
}

func (s *testTopSQLSuite) testSQLDurationSum(baseTs int, baseValue int) {
	w := NewMockResponseWriter()
	req, err := http.NewRequest(http.MethodGet, "/v1/sql_duration_sum", nil)
	s.NoError(err)
	urlQuery := url.Values{}
	urlQuery.Set("instance", s.tidbAddr)
	urlQuery.Set("start", strconv.Itoa(baseTs))
	urlQuery.Set("end", strconv.Itoa(baseTs+5))
	urlQuery.Set("window", "1s")
	req.URL.RawQuery = urlQuery.Encode()
	s.ng.ServeHTTP(w, req)
	if w.StatusCode != http.StatusOK {
		s.FailNow(fmt.Sprintf("http: %d, body: %s\n", w.StatusCode, string(w.Body)))
	}
	resp := metricsHttpResponse{}
	s.NoError(json.Unmarshal(w.Body, &resp))
	if resp.Status != "ok" {
		s.FailNow(fmt.Sprintf("status: %s, message: %s, body: %v\n", resp.Status, resp.Message, string(w.Body)))
	}
	s.Len(resp.Data, 1)
	s.Len(resp.Data[0].Plans, 1)
	s.Len(resp.Data[0].Plans[0].TimestampSecs, 5)
	s.Len(resp.Data[0].Plans[0].SQLDurationSum, 5)
	for n := 0; n < 5; n++ {
		s.Equal(uint64(baseTs+n), resp.Data[0].Plans[0].TimestampSecs[n])
		s.Equal(uint64(baseValue+n), resp.Data[0].Plans[0].SQLDurationSum[n])
	}
}

func (s *testTopSQLSuite) encodeTag(sql, plan []byte, label tipb.ResourceGroupTagLabel) []byte {
	b, err := proto.Marshal(&tipb.ResourceGroupTag{
		SqlDigest:  sql,
		PlanDigest: plan,
		Label:      &label,
	})
	s.NoError(err)
	return b
}
