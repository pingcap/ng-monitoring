package store

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"net/http"

	"github.com/genjidb/genji"
	rsmetering "github.com/pingcap/kvproto/pkg/resource_usage_agent"
	"github.com/pingcap/log"
	"github.com/pingcap/ng-monitoring/utils"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

var (
	bytesP  = utils.BytesBufferPool{}
	headerP = utils.HeaderPool{}
)

type DefaultStore struct {
	vminsertHandler http.HandlerFunc
	documentDB      *genji.DB
}

func NewDefaultStore(vminsertHandler http.HandlerFunc, documentDB *genji.DB) (*DefaultStore, error) {
	ds := &DefaultStore{
		vminsertHandler: vminsertHandler,
		documentDB:      documentDB,
	}
	if err := ds.initDocumentDB(); err != nil {
		return nil, err
	}
	return &DefaultStore{
		vminsertHandler: vminsertHandler,
		documentDB:      documentDB,
	}, nil
}

func (ds *DefaultStore) initDocumentDB() error {
	createTableStmts := []string{
		"CREATE TABLE IF NOT EXISTS sql_digest (digest VARCHAR(255) PRIMARY KEY)",
		"CREATE TABLE IF NOT EXISTS plan_digest (digest VARCHAR(255) PRIMARY KEY)",
	}

	for _, stmt := range createTableStmts {
		if err := ds.documentDB.Exec(stmt); err != nil {
			return err
		}
	}

	return nil
}

var _ Store = &DefaultStore{}

func (ds *DefaultStore) Instances(items []InstanceItem) error {
	m := instancesItemToMetric(items)
	if err := ds.writeTimeseriesDB(m...); err != nil {
		return err
	}

	return nil
}

func (ds *DefaultStore) TopSQLRecord(instance, instanceType string, record *tipb.CPUTimeRecord) error {
	m := topSQLProtoToMetric(instance, instanceType, record)
	return ds.writeTimeseriesDB(m)
}

func (ds *DefaultStore) ResourceMeteringRecord(
	instance, instanceType string,
	record *rsmetering.ResourceUsageRecord,
) error {
	ms, err := rsMeteringProtoToMetrics(instance, instanceType, record)
	if err != nil {
		return err
	}
	if ms != nil {
		if err := ds.writeTimeseriesDB(ms...); err != nil {
			return err
		}
	}
	return nil
}

func (ds *DefaultStore) SQLMeta(meta *tipb.SQLMeta) error {
	prepareStmt := "INSERT INTO sql_digest(digest, sql_text, is_internal) VALUES (?, ?, ?) ON CONFLICT DO NOTHING"
	prepare, err := ds.documentDB.Prepare(prepareStmt)
	if err != nil {
		return err
	}

	return prepare.Exec(hex.EncodeToString(meta.SqlDigest), meta.NormalizedSql, meta.IsInternalSql)
}

func (ds *DefaultStore) PlanMeta(meta *tipb.PlanMeta) error {
	prepareStmt := "INSERT INTO plan_digest(digest, plan_text) VALUES (?, ?) ON CONFLICT DO NOTHING"
	prepare, err := ds.documentDB.Prepare(prepareStmt)
	if err != nil {
		return err
	}

	return prepare.Exec(hex.EncodeToString(meta.PlanDigest), meta.NormalizedPlan)
}

func (ds *DefaultStore) Close() {}

// transform InstanceItem to Metric
func instancesItemToMetric(items []InstanceItem) (res []Metric) {
	for _, item := range items {
		var m Metric
		metric := instanceTags{
			Name:         MetricNameInstance,
			Instance:     item.Instance,
			InstanceType: item.InstanceType,
		}
		m.Metric = metric
		m.Timestamps = append(m.Timestamps, item.TimestampSecs*1000)
		m.Values = append(m.Values, 1)
		res = append(res, m)
	}

	return
}

// transform tipb.CPUTimeRecord to util.Metric
func topSQLProtoToMetric(
	instance, instanceType string,
	record *tipb.CPUTimeRecord,
) (m Metric) {
	metric := recordTags{
		Name:         MetricNameCPUTime,
		Instance:     instance,
		InstanceType: instanceType,
		SQLDigest:    hex.EncodeToString(record.SqlDigest),
		PlanDigest:   hex.EncodeToString(record.PlanDigest),
	}
	m.Metric = metric

	for i := range record.RecordListCpuTimeMs {
		tsMillis := record.RecordListTimestampSec[i] * 1000
		cpuTime := record.RecordListCpuTimeMs[i]

		m.Timestamps = append(m.Timestamps, tsMillis)
		m.Values = append(m.Values, cpuTime)
	}

	return
}

// transform resource_usage_agent.CPUTimeRecord to util.Metric
func rsMeteringProtoToMetrics(
	instance, instanceType string,
	record *rsmetering.ResourceUsageRecord,
) (ms []Metric, err error) {
	tag := tipb.ResourceGroupTag{}
	tag.Reset()
	if err = tag.Unmarshal(record.ResourceGroupTag); err != nil {
		return
	}
	sqlDigest := hex.EncodeToString(tag.SqlDigest)
	planDigest := hex.EncodeToString(tag.PlanDigest)

	mCpu := Metric{
		Metric: recordTags{
			Name:         MetricNameCPUTime,
			Instance:     instance,
			InstanceType: instanceType,
			SQLDigest:    sqlDigest,
			PlanDigest:   planDigest,
		},
	}
	mReadRow := Metric{
		Metric: recordTags{
			Name:         MetricNameReadRow,
			Instance:     instance,
			InstanceType: instanceType,
			SQLDigest:    sqlDigest,
			PlanDigest:   planDigest,
		},
	}
	mReadIndex := Metric{
		Metric: recordTags{
			Name:         MetricNameReadIndex,
			Instance:     instance,
			InstanceType: instanceType,
			SQLDigest:    sqlDigest,
			PlanDigest:   planDigest,
		},
	}
	mWriteRow := Metric{
		Metric: recordTags{
			Name:         MetricNameWriteRow,
			Instance:     instance,
			InstanceType: instanceType,
			SQLDigest:    sqlDigest,
			PlanDigest:   planDigest,
		},
	}
	mWriteIndex := Metric{
		Metric: recordTags{
			Name:         MetricNameWriteIndex,
			Instance:     instance,
			InstanceType: instanceType,
			SQLDigest:    sqlDigest,
			PlanDigest:   planDigest,
		},
	}

	for i := range record.RecordListTimestampSec {
		tsMillis := record.RecordListTimestampSec[i] * 1000
		appendMetricCPUTime(i, tsMillis, record.RecordListCpuTimeMs, &mCpu)
		appendMetricRowIndex(i, tsMillis, record.RecordListReadKeys, &mReadRow, &mReadIndex, tag.Label)
		appendMetricRowIndex(i, tsMillis, record.RecordListWriteKeys, &mWriteRow, &mWriteIndex, tag.Label)
	}

	ms = append(ms, mCpu, mReadRow, mReadIndex, mWriteRow, mWriteIndex)
	return
}

// appendMetricCPUTime only used in rsMeteringProtoToMetrics.
func appendMetricCPUTime(i int, ts uint64, values []uint32, mCpu *Metric) {
	var value uint32
	if len(values) > i {
		value = values[i]
	}
	mCpu.Timestamps = append(mCpu.Timestamps, ts)
	mCpu.Values = append(mCpu.Values, value)
}

// appendMetricRowIndex only used in rsMeteringProtoToMetrics, just used to reduce repetition.
func appendMetricRowIndex(i int, ts uint64, values []uint32, mRow, mIndex *Metric, label *tipb.ResourceGroupTagLabel) {
	var rows, indexes uint32
	if len(values) > i {
		if label != nil {
			if *label == tipb.ResourceGroupTagLabel_ResourceGroupTagLabelRow {
				rows = values[i]
			} else if *label == tipb.ResourceGroupTagLabel_ResourceGroupTagLabelIndex {
				indexes = values[i]
			}
		}
	}
	mRow.Timestamps = append(mRow.Timestamps, ts)
	mRow.Values = append(mRow.Values, rows)
	mIndex.Timestamps = append(mIndex.Timestamps, ts)
	mIndex.Values = append(mIndex.Values, indexes)
}

func (ds *DefaultStore) writeTimeseriesDB(metric ...Metric) error {
	bufReq := bytesP.Get()
	bufResp := bytesP.Get()
	header := headerP.Get()

	defer bytesP.Put(bufReq)
	defer bytesP.Put(bufResp)
	defer headerP.Put(header)

	if err := encodeMetric(bufReq, metric); err != nil {
		return err
	}

	respR := utils.NewRespWriter(bufResp, header)
	req, err := http.NewRequest("POST", "/api/v1/import", bufReq)
	if err != nil {
		return err
	}
	ds.vminsertHandler(&respR, req)

	if statusOK := respR.Code >= 200 && respR.Code < 300; !statusOK {
		log.Warn("failed to write timeseries db", zap.String("error", respR.Body.String()))
	}
	return nil
}

func encodeMetric(buf *bytes.Buffer, metrics []Metric) error {
	for _, m := range metrics {
		encoder := json.NewEncoder(buf)
		if err := encoder.Encode(m); err != nil {
			return err
		}
	}

	return nil
}
