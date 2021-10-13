package topsql

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"net/http"

	"github.com/zhongzc/ng_monitoring/utils"

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
	metricsP       = MetricSlicePool{}
	stringBuilderP = StringBuilderPool{}
	prepareSliceP  = PrepareSlicePool{}
)

func Init(vminsertHandler_ http.HandlerFunc, documentDB *genji.DB) {
	vminsertHandler = vminsertHandler_
	if err := initDocumentDB(documentDB); err != nil {
		log.Fatal("cannot init tables", zap.Error(err))
	}
}

func TopSQLRecords(records []*tipb.CPUTimeRecord) error {
	if len(records) == 0 {
		return nil
	}

	var err error
	err = insert(
		"INSERT INTO instance(instance, job) VALUES ",
		"(?, ?)", len(records),
		" ON CONFLICT DO NOTHING",
		func(target *[]interface{}) {
			for _, record := range records {
				*target = append(*target, record.Instance)
				*target = append(*target, record.Job)
			}
		},
	)
	if err != nil {
		return err
	}

	err = storeRecords(func(target *[]Metric) error {
		fillTopSQLProtoToMetric(records, target)
		return nil
	})
	return err
}

func ResourceMeteringRecords(records []*rsmetering.CPUTimeRecord) error {
	if len(records) == 0 {
		return nil
	}

	var err error
	err = insert(
		"INSERT INTO instance(instance, job) VALUES ",
		"(?, ?)", len(records),
		" ON CONFLICT DO NOTHING",
		func(target *[]interface{}) {
			for _, record := range records {
				*target = append(*target, record.Instance)
				*target = append(*target, record.Job)
			}
		},
	)
	if err != nil {
		return err
	}

	err = storeRecords(func(target *[]Metric) error {
		return fillRsMeteringProtoToMetric(records, target)
	})
	return err
}

func SQLMetas(metas []*tipb.SQLMeta) error {
	if len(metas) == 0 {
		return nil
	}

	return insert(
		"INSERT INTO sql_digest(digest, sql_text, is_internal) VALUES ",
		"(?, ?, ?)", len(metas),
		" ON CONFLICT DO NOTHING",
		func(target *[]interface{}) {
			for _, meta := range metas {
				*target = append(*target, hex.EncodeToString(meta.SqlDigest))
				*target = append(*target, meta.NormalizedSql)
				*target = append(*target, meta.IsInternalSql)
			}
		},
	)
}

func PlanMetas(metas []*tipb.PlanMeta) error {
	if len(metas) == 0 {
		return nil
	}

	return insert(
		"INSERT INTO plan_digest(digest, plan_text) VALUES ",
		"(?, ?)", len(metas),
		" ON CONFLICT DO NOTHING",
		func(target *[]interface{}) {
			for _, meta := range metas {
				*target = append(*target, hex.EncodeToString(meta.PlanDigest))
				*target = append(*target, meta.NormalizedPlan)
			}
		},
	)
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

func storeRecords(fill func(target *[]Metric) error) error {
	metrics := metricsP.Get()
	defer metricsP.Put(metrics)

	if err := fill(metrics); err != nil {
		return err
	}
	return writeTimeseriesDB(*metrics)
}

// transform tipb.CPUTimeRecord to util.Metric
func fillTopSQLProtoToMetric(
	records []*tipb.CPUTimeRecord,
	target *[]Metric,
) {
	for _, rawRecord := range records {
		*target = append(*target, Metric{})
		m := &(*target)[len(*target)-1]

		m.Metric.Name = "cpu_time"
		m.Metric.Instance = rawRecord.Instance
		m.Metric.Job = rawRecord.Job
		m.Metric.SQLDigest = hex.EncodeToString(rawRecord.SqlDigest)
		m.Metric.PlanDigest = hex.EncodeToString(rawRecord.PlanDigest)

		for i := range rawRecord.RecordListCpuTimeMs {
			tsMillis := rawRecord.RecordListTimestampSec[i] * 1000
			cpuTime := rawRecord.RecordListCpuTimeMs[i]

			m.Timestamps = append(m.Timestamps, tsMillis)
			m.Values = append(m.Values, cpuTime)
		}
	}
}

// transform resource_usage_agent.CPUTimeRecord to util.Metric
func fillRsMeteringProtoToMetric(
	records []*rsmetering.CPUTimeRecord,
	target *[]Metric,
) error {
	tag := tipb.ResourceGroupTag{}

	for _, rawRecord := range records {
		*target = append(*target, Metric{})
		m := &(*target)[len(*target)-1]

		m.Metric.Name = "cpu_time"
		m.Metric.Instance = rawRecord.Instance
		m.Metric.Job = rawRecord.Job

		tag.Reset()
		if err := tag.Unmarshal(rawRecord.ResourceGroupTag); err != nil {
			return err
		}

		m.Metric.SQLDigest = hex.EncodeToString(tag.SqlDigest)
		m.Metric.PlanDigest = hex.EncodeToString(tag.PlanDigest)

		for i := range rawRecord.RecordListCpuTimeMs {
			tsMillis := rawRecord.RecordListTimestampSec[i] * 1000
			cpuTime := rawRecord.RecordListCpuTimeMs[i]

			m.Timestamps = append(m.Timestamps, tsMillis)
			m.Values = append(m.Values, cpuTime)
		}
	}

	return nil
}

func writeTimeseriesDB(metrics []Metric) error {
	bufReq := bytesP.Get()
	bufResp := bytesP.Get()
	header := headerP.Get()

	defer bytesP.Put(bufReq)
	defer bytesP.Put(bufResp)
	defer headerP.Put(header)

	if err := encodeMetrics(bufReq, metrics); err != nil {
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

func encodeMetrics(buf *bytes.Buffer, metrics []Metric) error {
	encoder := json.NewEncoder(buf)
	for _, m := range metrics {
		if err := encoder.Encode(m); err != nil {
			return err
		}
	}
	return nil
}
