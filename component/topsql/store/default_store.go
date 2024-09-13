package store

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strconv"
	"sync"

	"github.com/pingcap/ng-monitoring/component/subscriber/model"
	"github.com/pingcap/ng-monitoring/component/topology"
	"github.com/pingcap/ng-monitoring/component/topsql/codec/resource_group_tag"
	"github.com/pingcap/ng-monitoring/utils"

	"github.com/genjidb/genji"
	rsmetering "github.com/pingcap/kvproto/pkg/resource_usage_agent"
	"github.com/pingcap/log"
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
	ms := instancesItemToMetric(items)
	return ds.writeTimeseriesDB(ms)
}

func (ds *DefaultStore) TopSQLRecord(instance, instanceType string, record *tipb.TopSQLRecord) error {
	ms := topSQLProtoToMetrics(instance, instanceType, record)
	return ds.writeTimeseriesDB(ms)
}

func (ds *DefaultStore) ResourceMeteringRecord(
	instance, instanceType string,
	record *rsmetering.ResourceUsageRecord,
	schemaInfo *sync.Map,
) error {
	ms, err := rsMeteringProtoToMetrics(instance, instanceType, record, schemaInfo)
	if err != nil {
		return err
	}
	return ds.writeTimeseriesDB(ms)
}

func (ds *DefaultStore) SQLMeta(meta *tipb.SQLMeta) error {
	prepareStmt := "INSERT INTO sql_digest(digest, sql_text, is_internal) VALUES (?, ?, ?) ON CONFLICT DO REPLACE"
	prepare, err := ds.documentDB.Prepare(prepareStmt)
	if err != nil {
		return err
	}

	return prepare.Exec(hex.EncodeToString(meta.SqlDigest), meta.NormalizedSql, meta.IsInternalSql)
}

func (ds *DefaultStore) PlanMeta(meta *tipb.PlanMeta) error {
	prepareStmt := "INSERT INTO plan_digest(digest, plan_text, encoded_plan) VALUES (?, ?, ?) ON CONFLICT DO REPLACE"
	prepare, err := ds.documentDB.Prepare(prepareStmt)
	if err != nil {
		return err
	}

	return prepare.Exec(hex.EncodeToString(meta.PlanDigest), meta.NormalizedPlan, meta.EncodedNormalizedPlan)
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
		m.TimestampMs = append(m.TimestampMs, item.TimestampSec*1000)
		m.Values = append(m.Values, 1)
		res = append(res, m)
	}

	return
}

// transform tipb.CPUTimeRecord to util.Metric
func topSQLProtoToMetrics(
	instance, instanceType string,
	record *tipb.TopSQLRecord,
) (ms []Metric) {
	sqlDigest := hex.EncodeToString(record.SqlDigest)
	planDigest := hex.EncodeToString(record.PlanDigest)

	mCpu := Metric{
		Metric: recordTags{
			Name:         MetricNameCPUTime,
			Instance:     instance,
			InstanceType: instanceType,
			SQLDigest:    sqlDigest,
			PlanDigest:   planDigest,
		},
	}
	mExecCount := Metric{
		Metric: recordTags{
			Name:         MetricNameSQLExecCount,
			Instance:     instance,
			InstanceType: instanceType,
			SQLDigest:    sqlDigest,
			PlanDigest:   planDigest,
		},
	}
	mDurationSum := Metric{
		Metric: recordTags{
			Name:         MetricNameSQLDurationSum,
			Instance:     instance,
			InstanceType: instanceType,
			SQLDigest:    sqlDigest,
			PlanDigest:   planDigest,
		},
	}
	mDurationCount := Metric{
		Metric: recordTags{
			Name:         MetricNameSQLDurationCount,
			Instance:     instance,
			InstanceType: instanceType,
			SQLDigest:    sqlDigest,
			PlanDigest:   planDigest,
		},
	}
	mKvExecCount := map[string]*Metric{}

	for _, item := range record.Items {
		tsMillis := item.TimestampSec * 1000

		mCpu.TimestampMs = append(mCpu.TimestampMs, tsMillis)
		mCpu.Values = append(mCpu.Values, uint64(item.CpuTimeMs))

		mExecCount.TimestampMs = append(mExecCount.TimestampMs, tsMillis)
		mExecCount.Values = append(mExecCount.Values, item.StmtExecCount)

		mDurationSum.TimestampMs = append(mDurationSum.TimestampMs, tsMillis)
		mDurationSum.Values = append(mDurationSum.Values, item.StmtDurationSumNs)

		mDurationCount.TimestampMs = append(mDurationCount.TimestampMs, tsMillis)
		mDurationCount.Values = append(mDurationCount.Values, item.StmtDurationCount)

		for target, execCount := range item.StmtKvExecCount {
			metric, ok := mKvExecCount[target]
			if !ok {
				mKvExecCount[target] = &Metric{
					Metric: recordTags{
						Name:         MetricNameSQLExecCount,
						Instance:     target,
						InstanceType: topology.ComponentTiKV,
						SQLDigest:    sqlDigest,
						PlanDigest:   planDigest,
					},
				}
				metric = mKvExecCount[target]
			}
			metric.TimestampMs = append(metric.TimestampMs, tsMillis)
			metric.Values = append(metric.Values, execCount)
		}
	}

	ms = append(ms, mCpu, mExecCount, mDurationSum, mDurationCount)
	for _, m := range mKvExecCount {
		ms = append(ms, *m)
	}
	return
}

// transform resource_usage_agent.ResourceUsageRecord to metrics
func rsMeteringProtoToMetrics(
	instance, instanceType string,
	record *rsmetering.ResourceUsageRecord,
	schemaInfo *sync.Map,
) (ms []Metric, err error) {
	var tag tipb.ResourceGroupTag
	tag, err = resource_group_tag.Decode(record.GetRecord().ResourceGroupTag)
	if err != nil {
		return
	}

	sqlDigest := hex.EncodeToString(tag.SqlDigest)
	planDigest := hex.EncodeToString(tag.PlanDigest)
	tableId := tag.TableId
	schemaName := "unknown"
	tableName := strconv.FormatInt(tableId, 10)
	if schemaInfo != nil {
		v, ok := schemaInfo.Load(tableId)
		if ok {
			if val, ok := v.(*model.TableDetail); ok {
				schemaName = val.DB
				tableName = tableName + "-" + val.Name
			}
		}
	}
	mCpu := Metric{
		Metric: recordTags{
			Name:         MetricNameCPUTime,
			Instance:     instance,
			InstanceType: instanceType,
			SQLDigest:    sqlDigest,
			PlanDigest:   planDigest,
			DB:           schemaName,
			Table:        tableName,
		},
	}
	mReadRow := Metric{
		Metric: recordTags{
			Name:         MetricNameReadRow,
			Instance:     instance,
			InstanceType: instanceType,
			SQLDigest:    sqlDigest,
			PlanDigest:   planDigest,
			DB:           schemaName,
			Table:        tableName,
		},
	}
	mReadIndex := Metric{
		Metric: recordTags{
			Name:         MetricNameReadIndex,
			Instance:     instance,
			InstanceType: instanceType,
			SQLDigest:    sqlDigest,
			PlanDigest:   planDigest,
			DB:           schemaName,
			Table:        tableName,
		},
	}
	mWriteRow := Metric{
		Metric: recordTags{
			Name:         MetricNameWriteRow,
			Instance:     instance,
			InstanceType: instanceType,
			SQLDigest:    sqlDigest,
			PlanDigest:   planDigest,
			DB:           schemaName,
			Table:        tableName,
		},
	}
	mWriteIndex := Metric{
		Metric: recordTags{
			Name:         MetricNameWriteIndex,
			Instance:     instance,
			InstanceType: instanceType,
			SQLDigest:    sqlDigest,
			PlanDigest:   planDigest,
			DB:           schemaName,
			Table:        tableName,
		},
	}

	for _, item := range record.GetRecord().GetItems() {
		tsMillis := item.TimestampSec * 1000

		mCpu.TimestampMs = append(mCpu.TimestampMs, tsMillis)
		mCpu.Values = append(mCpu.Values, uint64(item.CpuTimeMs))

		appendMetricRowIndex(tsMillis, item.ReadKeys, &mReadRow, &mReadIndex, tag.Label)
		appendMetricRowIndex(tsMillis, item.WriteKeys, &mWriteRow, &mWriteIndex, tag.Label)
	}
	ms = append(ms, mCpu, mReadRow, mReadIndex, mWriteRow, mWriteIndex)
	return
}

// appendMetricRowIndex only used in rsMeteringProtoToMetrics, just used to reduce repetition.
func appendMetricRowIndex(ts uint64, value uint32, mRow, mIndex *Metric, label *tipb.ResourceGroupTagLabel) {
	var rows, indexes uint32
	if label != nil {
		if *label == tipb.ResourceGroupTagLabel_ResourceGroupTagLabelRow {
			rows = value
		} else if *label == tipb.ResourceGroupTagLabel_ResourceGroupTagLabelIndex {
			indexes = value
		}
	}
	mRow.TimestampMs = append(mRow.TimestampMs, ts)
	mRow.Values = append(mRow.Values, uint64(rows))
	mIndex.TimestampMs = append(mIndex.TimestampMs, ts)
	mIndex.Values = append(mIndex.Values, uint64(indexes))
}

func (ds *DefaultStore) writeTimeseriesDB(metrics []Metric) error {
	if len(metrics) == 0 {
		return nil
	}

	bufReq := bytesP.Get()
	bufResp := bytesP.Get()
	header := headerP.Get()

	defer bytesP.Put(bufReq)
	defer bytesP.Put(bufResp)
	defer headerP.Put(header)
	if err := encodeMetric(bufReq, metrics); err != nil {
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
