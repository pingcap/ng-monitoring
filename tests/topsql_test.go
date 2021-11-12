package tests

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
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
	"github.com/pingcap/ng_monitoring/component/topology"
	"github.com/pingcap/ng_monitoring/component/topsql"
	"github.com/pingcap/ng_monitoring/component/topsql/query"
	"github.com/pingcap/ng_monitoring/component/topsql/service"
	"github.com/pingcap/ng_monitoring/config"
	"github.com/pingcap/ng_monitoring/config/pdvariable"
	"github.com/pingcap/ng_monitoring/database/timeseries"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/suite"
)

func TestTopSQL(t *testing.T) {
	suite.Run(t, &testTopSQLSuite{})
}

// TODO: It is not elegant to open the local port for testing,
//       we may need to refactor the code to achieve loose-coupling.
const (
	testTsdbPath       = "/tmp/ng-monitoring-test/tsdb"
	testTiKVServerIP   = "127.0.0.1"
	testTiKVServerPort = 10283
)

var testTiKVAddr = fmt.Sprintf("%s:%d", testTiKVServerIP, testTiKVServerPort)

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
	tikvServer *MockTiKVServer
	topCh      chan []topology.Component
	varCh      chan *pdvariable.PDVariable
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

	// init local mock tikv server
	s.tikvServer = NewMockTiKVServer()
	go func() {
		s.NoError(s.tikvServer.Serve(testTiKVServerPort))
	}()
	time.Sleep(time.Second) // wait for grpc server ready (ugly code)

	// init genji in memory
	var err error
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
	s.topCh = make(chan []topology.Component)
	s.varCh = make(chan *pdvariable.PDVariable)
	topsql.Init(s.db, timeseries.InsertHandler, timeseries.SelectHandler, s.topCh, s.varCh)
	s.varCh <- &pdvariable.PDVariable{EnableTopSQL: true}
	time.Sleep(100 * time.Millisecond)
	s.topCh <- []topology.Component{{
		Name:       topology.ComponentTiKV,
		IP:         testTiKVServerIP,
		Port:       testTiKVServerPort,
		StatusPort: 0,
	}}
	time.Sleep(100 * time.Millisecond)

	// init data
	s.tikvServer.PushRecords([]*rua.ResourceUsageRecord{{
		ResourceGroupTag:       s.encodeTag([]byte("sql-1"), []byte("plan-1"), tipb.ResourceGroupTagLabel_ResourceGroupTagLabelRow),
		RecordListTimestampSec: []uint64{1636000111, 1636000112, 1636000113, 1636000114, 1636000115},
		RecordListCpuTimeMs:    []uint32{121, 122, 123, 124, 125},
		RecordListReadKeys:     []uint32{131, 132, 133, 134, 135},
		RecordListWriteKeys:    []uint32{141, 142, 143, 144, 145},
	}, {
		ResourceGroupTag:       s.encodeTag([]byte("sql-2"), []byte("plan-2"), tipb.ResourceGroupTagLabel_ResourceGroupTagLabelIndex),
		RecordListTimestampSec: []uint64{1636000211, 1636000212, 1636000213, 1636000214, 1636000215},
		RecordListCpuTimeMs:    []uint32{221, 222, 223, 224, 225},
		RecordListReadKeys:     []uint32{231, 232, 233, 234, 235},
		RecordListWriteKeys:    []uint32{241, 242, 243, 244, 245},
	}, {
		// Unknown label will not be counted in the read_row/read_index/write_row/write_index.
		ResourceGroupTag:       s.encodeTag([]byte("sql-3"), []byte("plan-3"), tipb.ResourceGroupTagLabel_ResourceGroupTagLabelUnknown),
		RecordListTimestampSec: []uint64{1636000311, 1636000312, 1636000313, 1636000314, 1636000315},
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
	ctx, _ := gin.CreateTestContext(w)
	service.InstancesHandler(ctx)
	if w.StatusCode != http.StatusOK {
		s.FailNow(fmt.Sprintf("http: %d, body: %s\n", w.StatusCode, string(w.Body)))
	}
	resp := instancesHttpResponse{}
	s.NoError(json.Unmarshal(w.Body, &resp))
	if resp.Status != "ok" {
		s.FailNow(fmt.Sprintf("status: %s, message: %s, body: %v\n", resp.Status, resp.Message, string(w.Body)))
	}
	s.Len(resp.Data, 1)
	s.Equal(topology.ComponentTiKV, resp.Data[0].InstanceType)
	s.Equal(testTiKVAddr, resp.Data[0].Instance)
}

func (s *testTopSQLSuite) TestCpuTime() {
	s.testCpuTime(1636000111, 121)
	s.testCpuTime(1636000211, 221)
	s.testCpuTime(1636000311, 321)
}

func (s *testTopSQLSuite) TestReadRow() {
	s.testReadRow(1636000111, 131) // ResourceGroupTagLabelRow
	s.testReadRow(1636000211, 0)   // ResourceGroupTagLabelIndex
	s.testReadRow(1636000311, 0)   // ResourceGroupTagLabelUnknown
}

func (s *testTopSQLSuite) TestReadIndex() {
	s.testReadIndex(1636000111, 0)   // ResourceGroupTagLabelRow
	s.testReadIndex(1636000211, 231) // ResourceGroupTagLabelIndex
	s.testReadIndex(1636000311, 0)   // ResourceGroupTagLabelUnknown
}

func (s *testTopSQLSuite) TestWriteRow() {
	s.testWriteRow(1636000111, 141) // ResourceGroupTagLabelRow
	s.testWriteRow(1636000211, 0)   // ResourceGroupTagLabelIndex
	s.testWriteRow(1636000311, 0)   // ResourceGroupTagLabelUnknown
}

func (s *testTopSQLSuite) TestWriteIndex() {
	s.testWriteIndex(1636000111, 0)   // ResourceGroupTagLabelRow
	s.testWriteIndex(1636000211, 241) // ResourceGroupTagLabelIndex
	s.testWriteIndex(1636000311, 0)   // ResourceGroupTagLabelUnknown
}

func (s *testTopSQLSuite) testCpuTime(baseTs int, baseValue int) {
	var err error
	w := NewMockResponseWriter()
	ctx, _ := gin.CreateTestContext(w)
	ctx.Request, err = http.NewRequest(http.MethodGet, "", nil)
	s.NoError(err)
	urlQuery := url.Values{}
	urlQuery.Set("instance", testTiKVAddr)
	urlQuery.Set("start", strconv.Itoa(baseTs))
	urlQuery.Set("end", strconv.Itoa(baseTs+5))
	urlQuery.Set("window", "1s")
	ctx.Request.URL.RawQuery = urlQuery.Encode()
	service.CpuTimeHandler(ctx)
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
		s.Equal(uint32(baseValue+n), resp.Data[0].Plans[0].CPUTimeMillis[n])
	}
}

func (s *testTopSQLSuite) testReadRow(baseTs int, baseValue int) {
	var err error
	w := NewMockResponseWriter()
	ctx, _ := gin.CreateTestContext(w)
	ctx.Request, err = http.NewRequest(http.MethodGet, "", nil)
	s.NoError(err)
	urlQuery := url.Values{}
	urlQuery.Set("instance", testTiKVAddr)
	urlQuery.Set("start", strconv.Itoa(baseTs))
	urlQuery.Set("end", strconv.Itoa(baseTs+5))
	urlQuery.Set("window", "1s")
	ctx.Request.URL.RawQuery = urlQuery.Encode()
	service.ReadRowHandler(ctx)
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
			s.Equal(uint32(0), resp.Data[0].Plans[0].ReadRows[n])
		} else {
			s.Equal(uint32(baseValue+n), resp.Data[0].Plans[0].ReadRows[n])
		}
	}
}

func (s *testTopSQLSuite) testReadIndex(baseTs int, baseValue int) {
	var err error
	w := NewMockResponseWriter()
	ctx, _ := gin.CreateTestContext(w)
	ctx.Request, err = http.NewRequest(http.MethodGet, "", nil)
	s.NoError(err)
	urlQuery := url.Values{}
	urlQuery.Set("instance", testTiKVAddr)
	urlQuery.Set("start", strconv.Itoa(baseTs))
	urlQuery.Set("end", strconv.Itoa(baseTs+5))
	urlQuery.Set("window", "1s")
	ctx.Request.URL.RawQuery = urlQuery.Encode()
	service.ReadIndexHandler(ctx)
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
			s.Equal(uint32(0), resp.Data[0].Plans[0].ReadIndexes[n])
		} else {
			s.Equal(uint32(baseValue+n), resp.Data[0].Plans[0].ReadIndexes[n])
		}
	}
}

func (s *testTopSQLSuite) testWriteRow(baseTs int, baseValue int) {
	var err error
	w := NewMockResponseWriter()
	ctx, _ := gin.CreateTestContext(w)
	ctx.Request, err = http.NewRequest(http.MethodGet, "", nil)
	s.NoError(err)
	urlQuery := url.Values{}
	urlQuery.Set("instance", testTiKVAddr)
	urlQuery.Set("start", strconv.Itoa(baseTs))
	urlQuery.Set("end", strconv.Itoa(baseTs+5))
	urlQuery.Set("window", "1s")
	ctx.Request.URL.RawQuery = urlQuery.Encode()
	service.WriteRowHandler(ctx)
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
			s.Equal(uint32(0), resp.Data[0].Plans[0].WriteRows[n])
		} else {
			s.Equal(uint32(baseValue+n), resp.Data[0].Plans[0].WriteRows[n])
		}
	}
}

func (s *testTopSQLSuite) testWriteIndex(baseTs int, baseValue int) {
	var err error
	w := NewMockResponseWriter()
	ctx, _ := gin.CreateTestContext(w)
	ctx.Request, err = http.NewRequest(http.MethodGet, "", nil)
	s.NoError(err)
	urlQuery := url.Values{}
	urlQuery.Set("instance", testTiKVAddr)
	urlQuery.Set("start", strconv.Itoa(baseTs))
	urlQuery.Set("end", strconv.Itoa(baseTs+5))
	urlQuery.Set("window", "1s")
	ctx.Request.URL.RawQuery = urlQuery.Encode()
	service.WriteIndexHandler(ctx)
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
			s.Equal(uint32(0), resp.Data[0].Plans[0].WriteIndexes[n])
		} else {
			s.Equal(uint32(baseValue+n), resp.Data[0].Plans[0].WriteIndexes[n])
		}
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
