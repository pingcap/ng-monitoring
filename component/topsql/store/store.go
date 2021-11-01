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
	m, err := rsMeteringProtoToMetric(instance, instanceType, record)
	if err != nil {
		return err
	}
	return writeTimeseriesDB(m)
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
func rsMeteringProtoToMetric(
	instance, instance_type string,
	record *rsmetering.ResourceUsageRecord,
) (m Metric, err error) {
	tag := tipb.ResourceGroupTag{}

	m.Metric.Name = "cpu_time"
	m.Metric.Instance = instance
	m.Metric.InstanceType = instance_type

	tag.Reset()
	if err = tag.Unmarshal(record.ResourceGroupTag); err != nil {
		return
	}

	m.Metric.SQLDigest = hex.EncodeToString(tag.SqlDigest)
	m.Metric.PlanDigest = hex.EncodeToString(tag.PlanDigest)

	for i := range record.RecordListCpuTimeMs {
		tsMillis := record.RecordListTimestampSec[i] * 1000
		cpuTime := record.RecordListCpuTimeMs[i]

		m.Timestamps = append(m.Timestamps, tsMillis)
		m.Values = append(m.Values, cpuTime)
	}

	return
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
