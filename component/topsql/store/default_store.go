package store

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"net/http"

	"github.com/genjidb/genji"
	rsmetering "github.com/pingcap/kvproto/pkg/resource_usage_agent"
	"github.com/pingcap/log"
	"github.com/pingcap/ng-monitoring/component/topology"
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
		"CREATE TABLE IF NOT EXISTS instance (instance VARCHAR(255) PRIMARY KEY)",
	}

	for _, stmt := range createTableStmts {
		if err := ds.documentDB.Exec(stmt); err != nil {
			return err
		}
	}

	return nil
}

var _ Store = &DefaultStore{}

func (ds *DefaultStore) Instance(instance, instanceType string) error {
	prepareStmt := "INSERT INTO instance(instance, instance_type) VALUES (?, ?) ON CONFLICT DO NOTHING"
	prepare, err := ds.documentDB.Prepare(prepareStmt)
	if err != nil {
		return err
	}

	return prepare.Exec(instance, instanceType)
}

func (ds *DefaultStore) TopSQLRecord(instance, instanceType string, record *tipb.TopSQLRecord) error {
	ms := topSQLProtoToMetrics(instance, instanceType, record)
	for _, m := range ms {
		if err := ds.writeTimeseriesDB(m); err != nil {
			return err
		}
	}
	return nil
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
		for _, m := range ms {
			if err := ds.writeTimeseriesDB(m); err != nil {
				return err
			}
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

// transform tipb.TopSQLRecord to metrics
func topSQLProtoToMetrics(
	instance, instanceType string,
	record *tipb.TopSQLRecord,
) (ms []Metric) {
	sqlDigest := hex.EncodeToString(record.SqlDigest)
	planDigest := hex.EncodeToString(record.PlanDigest)

	mCpu := Metric{
		Metric: topSQLTags{
			Name:         MetricNameCPUTime,
			Instance:     instance,
			InstanceType: instanceType,
			SQLDigest:    sqlDigest,
			PlanDigest:   planDigest,
		},
	}
	mExecCount := Metric{
		Metric: topSQLTags{
			Name:         MetricNameSQLExecCount,
			Instance:     instance,
			InstanceType: instanceType,
			SQLDigest:    sqlDigest,
			PlanDigest:   planDigest,
		},
	}
	mDurationSum := Metric{
		Metric: topSQLTags{
			Name:         MetricNameSQLDurationSum,
			Instance:     instance,
			InstanceType: instanceType,
			SQLDigest:    sqlDigest,
			PlanDigest:   planDigest,
		},
	}
	mKvExecCount := map[string]*Metric{}

	for i := range record.RecordListCpuTimeMs {
		tsMillis := record.RecordListTimestampSec[i] * 1000
		appendMetricUint32List(i, tsMillis, record.RecordListCpuTimeMs, &mCpu)
		appendMetricUint64List(i, tsMillis, record.RecordListStmtExecCount, &mExecCount)
		appendMetricUint64List(i, tsMillis, record.RecordListStmtDurationSumNs, &mDurationSum)
		appendMetricKvExecCount(i, tsMillis, record.RecordListStmtKvExecCount, mKvExecCount, sqlDigest, planDigest)
	}

	ms = append(ms, mCpu, mExecCount, mDurationSum)
	for _, m := range mKvExecCount {
		ms = append(ms, *m)
	}
	return
}

// transform resource_usage_agent.ResourceUsageRecord to metrics
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
		Metric: topSQLTags{
			Name:         MetricNameCPUTime,
			Instance:     instance,
			InstanceType: instanceType,
			SQLDigest:    sqlDigest,
			PlanDigest:   planDigest,
		},
	}
	mReadRow := Metric{
		Metric: topSQLTags{
			Name:         MetricNameReadRow,
			Instance:     instance,
			InstanceType: instanceType,
			SQLDigest:    sqlDigest,
			PlanDigest:   planDigest,
		},
	}
	mReadIndex := Metric{
		Metric: topSQLTags{
			Name:         MetricNameReadIndex,
			Instance:     instance,
			InstanceType: instanceType,
			SQLDigest:    sqlDigest,
			PlanDigest:   planDigest,
		},
	}
	mWriteRow := Metric{
		Metric: topSQLTags{
			Name:         MetricNameWriteRow,
			Instance:     instance,
			InstanceType: instanceType,
			SQLDigest:    sqlDigest,
			PlanDigest:   planDigest,
		},
	}
	mWriteIndex := Metric{
		Metric: topSQLTags{
			Name:         MetricNameWriteIndex,
			Instance:     instance,
			InstanceType: instanceType,
			SQLDigest:    sqlDigest,
			PlanDigest:   planDigest,
		},
	}

	for i := range record.RecordListTimestampSec {
		tsMillis := record.RecordListTimestampSec[i] * 1000
		appendMetricUint32List(i, tsMillis, record.RecordListCpuTimeMs, &mCpu)
		appendMetricRowIndex(i, tsMillis, record.RecordListReadKeys, &mReadRow, &mReadIndex, tag.Label)
		appendMetricRowIndex(i, tsMillis, record.RecordListWriteKeys, &mWriteRow, &mWriteIndex, tag.Label)
	}

	ms = append(ms, mCpu, mReadRow, mReadIndex, mWriteRow, mWriteIndex)
	return
}

func appendMetricUint32List(i int, ts uint64, values []uint32, metric *Metric) {
	var value uint32
	if len(values) > i {
		value = values[i]
	}
	metric.Timestamps = append(metric.Timestamps, ts)
	metric.Values = append(metric.Values, uint64(value))
}

func appendMetricUint64List(i int, ts uint64, values []uint64, metric *Metric) {
	var value uint64
	if len(values) > i {
		value = values[i]
	}
	metric.Timestamps = append(metric.Timestamps, ts)
	metric.Values = append(metric.Values, value)
}

func appendMetricKvExecCount(i int, ts uint64, values []*tipb.TopSQLStmtKvExecCount, metrics map[string]*Metric, sqlDigest, planDigest string) {
	kvExecCount := values[i]
	for target, execCount := range kvExecCount.ExecCount {
		metric, ok := metrics[target]
		if !ok {
			metrics[target] = &Metric{
				Metric: topSQLTags{
					Name:         MetricNameSQLExecCount,
					Instance:     target,
					InstanceType: topology.ComponentTiKV,
					SQLDigest:    sqlDigest,
					PlanDigest:   planDigest,
				},
			}
			metric = metrics[target]
		}
		metric.Timestamps = append(metric.Timestamps, ts)
		metric.Values = append(metric.Values, execCount)
	}
}

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
	mRow.Values = append(mRow.Values, uint64(rows))
	mIndex.Timestamps = append(mIndex.Timestamps, ts)
	mIndex.Values = append(mIndex.Values, uint64(indexes))
}

func (ds *DefaultStore) writeTimeseriesDB(metric Metric) error {
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

func encodeMetric(buf *bytes.Buffer, metric Metric) error {
	encoder := json.NewEncoder(buf)
	return encoder.Encode(metric)
}
