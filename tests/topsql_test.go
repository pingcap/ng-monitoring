package tests

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang/protobuf/proto"
	rua "github.com/pingcap/kvproto/pkg/resource_usage_agent"
	"github.com/pingcap/ng-monitoring/component/topology"
	"github.com/pingcap/ng-monitoring/component/topsql"
	"github.com/pingcap/ng-monitoring/component/topsql/query"
	"github.com/pingcap/ng-monitoring/component/topsql/store"
	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/ng-monitoring/config/pdvariable"
	"github.com/pingcap/ng-monitoring/database"
	"github.com/pingcap/ng-monitoring/database/document"
	"github.com/pingcap/ng-monitoring/database/timeseries"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/suite"
)

func TestTopSQL(t *testing.T) {
	suite.Run(t, &testTopSQLSuite{})
}

var now = uint64(time.Now().Unix())
var testBaseTs = now - (now % 10000)

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
	Data []query.RecordItem `json:"data"`
}

type testTopSQLSuite struct {
	suite.Suite
	dir        string
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
	dir, err := ioutil.TempDir("", "topsql-test")
	s.NoError(err)
	s.dir = dir

	cfg := config.GetDefaultConfig()
	cfg.Storage.Path = dir
	config.StoreGlobalConfig(&cfg)

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

	database.Init(&cfg)

	// init topsql
	s.topCh = make(topology.Subscriber)
	s.varCh = make(pdvariable.Subscriber)
	s.cfgCh = make(config.Subscriber)
	err = topsql.Init(&cfg, s.cfgCh, document.Get(), timeseries.InsertHandler, timeseries.SelectHandler, s.topCh, s.varCh)
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
		SqlDigest:  []byte("sql-1"),
		PlanDigest: []byte("plan-1"),
		Items: []*tipb.TopSQLRecordItem{{
			TimestampSec:      testBaseTs + 111,
			CpuTimeMs:         121,
			StmtExecCount:     131,
			StmtDurationSumNs: 141,
			StmtKvExecCount:   map[string]uint64{"tikv-1": 151, "tikv-2": 251},
		}, {
			TimestampSec:      testBaseTs + 112,
			CpuTimeMs:         122,
			StmtExecCount:     132,
			StmtDurationSumNs: 142,
			StmtKvExecCount:   map[string]uint64{"tikv-1": 152, "tikv-2": 252},
		}, {
			TimestampSec:      testBaseTs + 113,
			CpuTimeMs:         123,
			StmtExecCount:     133,
			StmtDurationSumNs: 143,
			StmtKvExecCount:   map[string]uint64{"tikv-1": 153, "tikv-2": 253},
		}, {
			TimestampSec:      testBaseTs + 114,
			CpuTimeMs:         124,
			StmtExecCount:     134,
			StmtDurationSumNs: 144,
			StmtKvExecCount:   map[string]uint64{"tikv-1": 154, "tikv-2": 254},
		}, {
			TimestampSec:      testBaseTs + 115,
			CpuTimeMs:         125,
			StmtExecCount:     135,
			StmtDurationSumNs: 145,
			StmtKvExecCount:   map[string]uint64{"tikv-1": 155, "tikv-2": 255},
		}},
	}, {
		SqlDigest:  []byte("sql-1"),
		PlanDigest: []byte("plan-1"),
		Items: []*tipb.TopSQLRecordItem{{
			TimestampSec:      testBaseTs + 211,
			CpuTimeMs:         0,
			StmtExecCount:     0,
			StmtDurationSumNs: 10,
			StmtKvExecCount:   map[string]uint64{"tikv-1": 0, "tikv-2": 0},
		}, {
			TimestampSec:      testBaseTs + 212,
			CpuTimeMs:         1,
			StmtExecCount:     1,
			StmtDurationSumNs: 20,
			StmtKvExecCount:   map[string]uint64{"tikv-1": 1, "tikv-2": 1},
		}, {
			TimestampSec:      testBaseTs + 213,
			CpuTimeMs:         2,
			StmtExecCount:     2,
			StmtDurationSumNs: 30,
			StmtKvExecCount:   map[string]uint64{"tikv-1": 2, "tikv-2": 2},
		}, {
			TimestampSec:      testBaseTs + 214,
			CpuTimeMs:         3,
			StmtExecCount:     3,
			StmtDurationSumNs: 40,
			StmtKvExecCount:   map[string]uint64{"tikv-1": 3, "tikv-2": 3},
		}, {
			TimestampSec:      testBaseTs + 215,
			CpuTimeMs:         0,
			StmtExecCount:     0,
			StmtDurationSumNs: 50,
			StmtKvExecCount:   map[string]uint64{"tikv-1": 0, "tikv-2": 0},
		}},
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
	database.Stop()
	s.NoError(os.RemoveAll(s.dir))
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
	s.testCpuTime(s.tikvAddr, "tikv", testBaseTs+111, testBaseTs+116,
		[]uint64{testBaseTs + 111, testBaseTs + 112, testBaseTs + 113, testBaseTs + 114, testBaseTs + 115},
		[]uint64{121, 122, 123, 124, 125})
	s.testCpuTime(s.tikvAddr, "tikv", testBaseTs+211, testBaseTs+216,
		[]uint64{testBaseTs + 211, testBaseTs + 212, testBaseTs + 213, testBaseTs + 214, testBaseTs + 215},
		[]uint64{221, 222, 223, 224, 225})
	s.testCpuTime(s.tikvAddr, "tikv", testBaseTs+311, testBaseTs+316,
		[]uint64{testBaseTs + 311, testBaseTs + 312, testBaseTs + 313, testBaseTs + 314, testBaseTs + 315},
		[]uint64{321, 322, 323, 324, 325})
	s.testCpuTime(s.tidbAddr, "tidb", testBaseTs+211, testBaseTs+216,
		[]uint64{testBaseTs + 211, testBaseTs + 212, testBaseTs + 213, testBaseTs + 214, testBaseTs + 215}, // 3 items, not 5
		[]uint64{0, 1, 2, 3, 0})
}

func (s *testTopSQLSuite) TestReadRow() {
	s.testReadRow(s.tikvAddr, "tikv", testBaseTs+111, testBaseTs+116,
		[]uint64{testBaseTs + 111, testBaseTs + 112, testBaseTs + 113, testBaseTs + 114, testBaseTs + 115},
		[]uint64{131, 132, 133, 134, 135}) // ResourceGroupTagLabelRow
	s.testReadRow(s.tikvAddr, "tikv", testBaseTs+211, testBaseTs+216,
		[]uint64{testBaseTs + 211, testBaseTs + 212, testBaseTs + 213, testBaseTs + 214, testBaseTs + 215},
		[]uint64{0, 0, 0, 0, 0}) // ResourceGroupTagLabelIndex
	s.testReadRow(s.tikvAddr, "tikv", testBaseTs+311, testBaseTs+316,
		[]uint64{testBaseTs + 311, testBaseTs + 312, testBaseTs + 313, testBaseTs + 314, testBaseTs + 315},
		[]uint64{0, 0, 0, 0, 0}) // ResourceGroupTagLabelUnknown
}

func (s *testTopSQLSuite) TestReadIndex() {
	s.testReadIndex(s.tikvAddr, "tikv", testBaseTs+111, testBaseTs+116,
		[]uint64{testBaseTs + 111, testBaseTs + 112, testBaseTs + 113, testBaseTs + 114, testBaseTs + 115},
		[]uint64{0, 0, 0, 0, 0}) // ResourceGroupTagLabelRow
	s.testReadIndex(s.tikvAddr, "tikv", testBaseTs+211, testBaseTs+216,
		[]uint64{testBaseTs + 211, testBaseTs + 212, testBaseTs + 213, testBaseTs + 214, testBaseTs + 215},
		[]uint64{231, 232, 233, 234, 235}) // ResourceGroupTagLabelIndex
	s.testReadIndex(s.tikvAddr, "tikv", testBaseTs+311, testBaseTs+316,
		[]uint64{testBaseTs + 311, testBaseTs + 312, testBaseTs + 313, testBaseTs + 314, testBaseTs + 315},
		[]uint64{0, 0, 0, 0, 0}) // ResourceGroupTagLabelUnknown
}

func (s *testTopSQLSuite) TestWriteRow() {
	s.testWriteRow(s.tikvAddr, "tikv", testBaseTs+111, testBaseTs+116,
		[]uint64{testBaseTs + 111, testBaseTs + 112, testBaseTs + 113, testBaseTs + 114, testBaseTs + 115},
		[]uint64{141, 142, 143, 144, 145}) // ResourceGroupTagLabelRow
	s.testWriteRow(s.tikvAddr, "tikv", testBaseTs+211, testBaseTs+216,
		[]uint64{testBaseTs + 211, testBaseTs + 212, testBaseTs + 213, testBaseTs + 214, testBaseTs + 215},
		[]uint64{0, 0, 0, 0, 0}) // ResourceGroupTagLabelIndex
	s.testWriteRow(s.tikvAddr, "tikv", testBaseTs+311, testBaseTs+316,
		[]uint64{testBaseTs + 311, testBaseTs + 312, testBaseTs + 313, testBaseTs + 314, testBaseTs + 315},
		[]uint64{0, 0, 0, 0, 0}) // ResourceGroupTagLabelUnknown
}

func (s *testTopSQLSuite) TestWriteIndex() {
	s.testWriteIndex(s.tikvAddr, "tikv", testBaseTs+111, testBaseTs+116,
		[]uint64{testBaseTs + 111, testBaseTs + 112, testBaseTs + 113, testBaseTs + 114, testBaseTs + 115},
		[]uint64{0, 0, 0, 0, 0}) // ResourceGroupTagLabelRow
	s.testWriteIndex(s.tikvAddr, "tikv", testBaseTs+211, testBaseTs+216,
		[]uint64{testBaseTs + 211, testBaseTs + 212, testBaseTs + 213, testBaseTs + 214, testBaseTs + 215},
		[]uint64{241, 242, 243, 244, 245}) // ResourceGroupTagLabelIndex
	s.testWriteIndex(s.tikvAddr, "tikv", testBaseTs+311, testBaseTs+316,
		[]uint64{testBaseTs + 311, testBaseTs + 312, testBaseTs + 313, testBaseTs + 314, testBaseTs + 315},
		[]uint64{0, 0, 0, 0, 0}) // ResourceGroupTagLabelUnknown
}

func (s *testTopSQLSuite) TestSQLExecCount() {
	s.testSQLExecCount(s.tidbAddr, "tidb", testBaseTs+111, testBaseTs+116,
		[]uint64{testBaseTs + 111, testBaseTs + 112, testBaseTs + 113, testBaseTs + 114, testBaseTs + 115},
		[]uint64{131, 132, 133, 134, 135})
	s.testSQLExecCount(s.tidbAddr, "tidb", testBaseTs+211, testBaseTs+216,
		[]uint64{testBaseTs + 211, testBaseTs + 212, testBaseTs + 213, testBaseTs + 214, testBaseTs + 215},
		[]uint64{0, 1, 2, 3, 0})
	s.testSQLExecCount("tikv-1", "tikv", testBaseTs+111, testBaseTs+116,
		[]uint64{testBaseTs + 111, testBaseTs + 112, testBaseTs + 113, testBaseTs + 114, testBaseTs + 115},
		[]uint64{151, 152, 153, 154, 155})
	s.testSQLExecCount("tikv-2", "tikv", testBaseTs+111, testBaseTs+116,
		[]uint64{testBaseTs + 111, testBaseTs + 112, testBaseTs + 113, testBaseTs + 114, testBaseTs + 115},
		[]uint64{251, 252, 253, 254, 255})
	s.testSQLExecCount("tikv-1", "tikv", testBaseTs+211, testBaseTs+216,
		[]uint64{testBaseTs + 211, testBaseTs + 212, testBaseTs + 213, testBaseTs + 214, testBaseTs + 215},
		[]uint64{0, 1, 2, 3, 0})
	s.testSQLExecCount("tikv-2", "tikv", testBaseTs+211, testBaseTs+216,
		[]uint64{testBaseTs + 211, testBaseTs + 212, testBaseTs + 213, testBaseTs + 214, testBaseTs + 215},
		[]uint64{0, 1, 2, 3, 0})
}

func (s *testTopSQLSuite) TestSQLDurationSum() {
	s.testSQLDurationSum(s.tidbAddr, "tidb", testBaseTs+111, testBaseTs+116,
		[]uint64{testBaseTs + 111, testBaseTs + 112, testBaseTs + 113, testBaseTs + 114, testBaseTs + 115},
		[]uint64{141, 142, 143, 144, 145})
	s.testSQLDurationSum(s.tidbAddr, "tidb", testBaseTs+211, testBaseTs+216,
		[]uint64{testBaseTs + 211, testBaseTs + 212, testBaseTs + 213, testBaseTs + 214, testBaseTs + 215},
		[]uint64{10, 20, 30, 40, 50})
}

func (s *testTopSQLSuite) testCpuTime(instance, instanceType string, start, end uint64, ts []uint64, values []uint64) {
	r := s.doQuery(store.MetricNameCPUTime, instance, instanceType, start, end)
	s.Len(r, 1)
	s.Len(r[0].Plans, 1)
	s.Equal(r[0].Plans[0].TimestampSec, ts)
	s.Equal(r[0].Plans[0].CPUTimeMs, values)
}

func (s *testTopSQLSuite) testReadRow(instance, instanceType string, start, end uint64, ts []uint64, values []uint64) {
	r := s.doQuery(store.MetricNameReadRow, instance, instanceType, start, end)
	s.Len(r, 1)
	s.Len(r[0].Plans, 1)
	s.Equal(r[0].Plans[0].TimestampSec, ts)
	s.Equal(r[0].Plans[0].ReadRows, values)
}

func (s *testTopSQLSuite) testReadIndex(instance, instanceType string, start, end uint64, ts []uint64, values []uint64) {
	r := s.doQuery(store.MetricNameReadIndex, instance, instanceType, start, end)
	s.Len(r, 1)
	s.Len(r[0].Plans, 1)
	s.Equal(r[0].Plans[0].TimestampSec, ts)
	s.Equal(r[0].Plans[0].ReadIndexes, values)
}

func (s *testTopSQLSuite) testWriteRow(instance, instanceType string, start, end uint64, ts []uint64, values []uint64) {
	r := s.doQuery(store.MetricNameWriteRow, instance, instanceType, start, end)
	s.Len(r, 1)
	s.Len(r[0].Plans, 1)
	s.Equal(r[0].Plans[0].TimestampSec, ts)
	s.Equal(r[0].Plans[0].WriteRows, values)
}

func (s *testTopSQLSuite) testWriteIndex(instance, instanceType string, start, end uint64, ts []uint64, values []uint64) {
	r := s.doQuery(store.MetricNameWriteIndex, instance, instanceType, start, end)
	s.Len(r, 1)
	s.Len(r[0].Plans, 1)
	s.Equal(r[0].Plans[0].TimestampSec, ts)
	s.Equal(r[0].Plans[0].WriteIndexes, values)
}

func (s *testTopSQLSuite) testSQLExecCount(instance, instanceType string, start, end uint64, ts []uint64, values []uint64) {
	r := s.doQuery(store.MetricNameSQLExecCount, instance, instanceType, start, end)
	s.Len(r, 1)
	s.Len(r[0].Plans, 1)
	s.Equal(r[0].Plans[0].TimestampSec, ts)
	s.Equal(r[0].Plans[0].SQLExecCount, values)
}

func (s *testTopSQLSuite) testSQLDurationSum(instance, instanceType string, start, end uint64, ts []uint64, values []uint64) {
	r := s.doQuery(store.MetricNameSQLDurationSum, instance, instanceType, start, end)
	s.Len(r, 1)
	s.Len(r[0].Plans, 1)
	s.Equal(r[0].Plans[0].TimestampSec, ts)
	s.Equal(r[0].Plans[0].SQLDurationSum, values)
}

func (s *testTopSQLSuite) doQuery(name, instance, instanceType string, start, end uint64) []query.RecordItem {
	w := NewMockResponseWriter()
	req, err := http.NewRequest(http.MethodGet, "/v1/"+name, nil)
	s.NoError(err)
	urlQuery := url.Values{}
	urlQuery.Set("instance", instance)
	urlQuery.Set("instance_type", instanceType)
	urlQuery.Set("start", fmt.Sprintf("%d", start))
	urlQuery.Set("end", fmt.Sprintf("%d", end))
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
	return resp.Data
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
