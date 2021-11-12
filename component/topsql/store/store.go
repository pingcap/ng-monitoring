package store

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"net/http"

	"github.com/pingcap/ng_monitoring/utils"

	"github.com/genjidb/genji"
	rsmetering "github.com/pingcap/kvproto/pkg/resource_usage_agent"
	"github.com/pingcap/log"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

var (
	vminsertHandler http.HandlerFunc
	documentDB      *genji.DB

	bytesP         = utils.BytesBufferPool{}
	headerP        = utils.HeaderPool{}
	stringBuilderP = StringBuilderPool{}
	prepareSliceP  = PrepareSlicePool{}
)

func Init(vminsertHandler_ http.HandlerFunc, documentDB *genji.DB) {
	vminsertHandler = vminsertHandler_
	if err := initDocumentDB(documentDB); err != nil {
		log.Fatal("failed to create tables", zap.Error(err))
	}
}

func initDocumentDB(db *genji.DB) error {
	documentDB = db

	createTableStmts := []string{
		"CREATE TABLE IF NOT EXISTS sql_digest (digest VARCHAR(255) PRIMARY KEY)",
		"CREATE TABLE IF NOT EXISTS plan_digest (digest VARCHAR(255) PRIMARY KEY)",
		"CREATE TABLE IF NOT EXISTS instance (instance VARCHAR(255) PRIMARY KEY)",
	}

	for _, stmt := range createTableStmts {
		if err := db.Exec(stmt); err != nil {
			return err
		}
	}

	return nil
}

func Stop() {

}

func Instance(instance, instanceType string) error {
	prepareStmt := "INSERT INTO instance(instance, instance_type) VALUES (?, ?) ON CONFLICT DO NOTHING"
	prepare, err := documentDB.Prepare(prepareStmt)
	if err != nil {
		return err
	}

	return prepare.Exec(instance, instanceType)
}

func TopSQLRecord(instance, instanceType string, record *tipb.CPUTimeRecord) error {
	m := topSQLProtoToMetric(instance, instanceType, record)
	return writeTimeseriesDB(m)
}

func ResourceMeteringRecord(
	instance, instanceType string,
	record *rsmetering.ResourceUsageRecord,
) error {
	ms, err := rsMeteringProtoToMetrics(instance, instanceType, record)
	if err != nil {
		return err
	}
	if ms != nil {
		for _, m := range ms {
			if err := writeTimeseriesDB(m); err != nil {
				return err
			}
		}
	}
	return nil
}

func SQLMeta(meta *tipb.SQLMeta) error {
	prepareStmt := "INSERT INTO sql_digest(digest, sql_text, is_internal) VALUES (?, ?, ?) ON CONFLICT DO NOTHING"
	prepare, err := documentDB.Prepare(prepareStmt)
	if err != nil {
		return err
	}

	return prepare.Exec(hex.EncodeToString(meta.SqlDigest), meta.NormalizedSql, meta.IsInternalSql)
}

func PlanMeta(meta *tipb.PlanMeta) error {
	prepareStmt := "INSERT INTO plan_digest(digest, plan_text) VALUES (?, ?) ON CONFLICT DO NOTHING"
	prepare, err := documentDB.Prepare(prepareStmt)
	if err != nil {
		return err
	}

	return prepare.Exec(hex.EncodeToString(meta.PlanDigest), meta.NormalizedPlan)
}

func insert(
	header string, // INSERT INTO {table}({fields}...) VALUES
	elem string, times int, // (?, ?, ... , ?), (?, ?, ... , ?), ... (?, ?, ... , ?)
	footer string, // ON CONFLICT DO NOTHING
	fill func(target *[]interface{}),
) error {
	if times == 0 {
		log.Fatal("unexpected zero times", zap.Int("times", times))
	}

	prepareStmt := buildPrepareStmt(header, elem, times, footer)
	return execStmt(prepareStmt, fill)
}

func buildPrepareStmt(header string, elem string, times int, footer string) string {
	sb := stringBuilderP.Get()
	defer stringBuilderP.Put(sb)

	sb.WriteString(header)
	sb.WriteString(elem)
	for i := 0; i < times-1; i++ {
		sb.WriteString(", ")
		sb.WriteString(elem)
	}
	sb.WriteString(footer)

	return sb.String()
}

func execStmt(prepareStmt string, fill func(target *[]interface{})) error {
	stmt, err := documentDB.Prepare(prepareStmt)
	if err != nil {
		return err
	}

	ps := prepareSliceP.Get()
	defer prepareSliceP.Put(ps)

	fill(ps)
	return stmt.Exec(*ps...)
}

// transform tipb.CPUTimeRecord to util.Metric
func topSQLProtoToMetric(
	instance, instanceType string,
	record *tipb.CPUTimeRecord,
) (m Metric) {
	m.Metric.Name = "cpu_time"
	m.Metric.Instance = instance
	m.Metric.InstanceType = instanceType
	m.Metric.SQLDigest = hex.EncodeToString(record.SqlDigest)
	m.Metric.PlanDigest = hex.EncodeToString(record.PlanDigest)

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
	instance, instance_type string,
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
			Name:         "cpu_time",
			Instance:     instance,
			InstanceType: instance_type,
			SQLDigest:    sqlDigest,
			PlanDigest:   planDigest,
		},
	}
	mReadRow := Metric{
		Metric: topSQLTags{
			Name:         "read_row",
			Instance:     instance,
			InstanceType: instance_type,
			SQLDigest:    sqlDigest,
			PlanDigest:   planDigest,
		},
	}
	mReadIndex := Metric{
		Metric: topSQLTags{
			Name:         "read_index",
			Instance:     instance,
			InstanceType: instance_type,
			SQLDigest:    sqlDigest,
			PlanDigest:   planDigest,
		},
	}
	mWriteRow := Metric{
		Metric: topSQLTags{
			Name:         "write_row",
			Instance:     instance,
			InstanceType: instance_type,
			SQLDigest:    sqlDigest,
			PlanDigest:   planDigest,
		},
	}
	mWriteIndex := Metric{
		Metric: topSQLTags{
			Name:         "write_index",
			Instance:     instance,
			InstanceType: instance_type,
			SQLDigest:    sqlDigest,
			PlanDigest:   planDigest,
		},
	}

	for i := range record.RecordListCpuTimeMs {
		tsMillis := record.RecordListTimestampSec[i] * 1000
		cpuTime := record.RecordListCpuTimeMs[i]

		mCpu.Timestamps = append(mCpu.Timestamps, tsMillis)
		mCpu.Values = append(mCpu.Values, cpuTime)

		appendMetricRowIndex(i, tsMillis, record.RecordListReadKeys, &mReadRow, &mReadIndex, tag.Label)
		appendMetricRowIndex(i, tsMillis, record.RecordListWriteKeys, &mWriteRow, &mWriteIndex, tag.Label)
	}

	ms = append(ms, mCpu, mReadRow, mReadIndex, mWriteRow, mWriteIndex)
	return
}

// appendMetricRowIndex only used in rsMeteringProtoToMetrics, just used to reduce repetition.
func appendMetricRowIndex(i int, ts uint64, values []uint32, mRow, mIndex *Metric, label *tipb.ResourceGroupTagLabel) {
	var rows, indexes uint32
	if len(values) >= i {
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

func writeTimeseriesDB(metric Metric) error {
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
	vminsertHandler(&respR, req)

	if statusOK := respR.Code >= 200 && respR.Code < 300; !statusOK {
		log.Warn("failed to write timeseries db", zap.String("error", respR.Body.String()))
	}
	return nil
}

func encodeMetric(buf *bytes.Buffer, metric Metric) error {
	encoder := json.NewEncoder(buf)
	return encoder.Encode(metric)
}
