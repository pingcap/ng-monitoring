package docdb

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"path"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/pingcap/log"
	"github.com/pingcap/ng-monitoring/component/conprof/meta"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

type sqliteDB struct {
	db *sql.DB

	writeSQLMetaStmt  *sql.Stmt
	writePlanMetaStmt *sql.Stmt
}

func NewSQLiteDB(dbPath string, useWAL bool) (DocDB, error) {
	dbPath = path.Join(dbPath, "ng-sqlite.db")
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		log.Fatal("failed to open sqlite db", zap.String("path", dbPath), zap.Error(err))
	}
	if useWAL {
		_, err := db.Exec("PRAGMA journal_mode=WAL")
		if err != nil {
			return nil, err
		}
	}
	d := &sqliteDB{db: db}
	if err := d.tryInitTables(); err != nil {
		return nil, err
	}
	writeSQLMetaSQL := `INSERT OR REPLACE INTO sql_digest (digest, sql_text, is_internal, created_at_ts) VALUES (?, ?, ?, ?)`
	d.writeSQLMetaStmt, err = d.db.Prepare(writeSQLMetaSQL)
	if err != nil {
		return nil, err
	}
	writePlanMetaSQL := `INSERT OR REPLACE INTO plan_digest (digest, plan_text, encoded_plan, created_at_ts) VALUES (?, ?, ?, ?)`
	d.writePlanMetaStmt, err = d.db.Prepare(writePlanMetaSQL)
	if err != nil {
		return nil, err
	}
	return d, nil
}

func (d *sqliteDB) tryInitTables() error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS ng_monitoring_config (module TEXT primary key, config TEXT)`,
		`CREATE TABLE IF NOT EXISTS sql_digest (digest VARCHAR(255) PRIMARY KEY, sql_text TEXT, is_internal BOOLEAN, created_at_ts INTEGER)`,
		`CREATE TABLE IF NOT EXISTS plan_digest (digest VARCHAR(255) PRIMARY KEY, plan_text TEXT, encoded_plan TEXT, created_at_ts INTEGER)`,
		`CREATE TABLE IF NOT EXISTS conprof_targets_meta (id INTEGER PRIMARY KEY, kind TEXT, component TEXT, address TEXT, last_scrape_ts INTEGER)`,
		`CREATE INDEX IF NOT EXISTS idx_sql_digest_ts ON sql_digest (created_at_ts)`,
		`CREATE INDEX IF NOT EXISTS idx_plan_digest_ts ON plan_digest (created_at_ts)`,
	}
	for _, stmt := range stmts {
		if _, err := d.db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func (d *sqliteDB) Close() error {
	return d.db.Close()
}

func (d *sqliteDB) SaveConfig(ctx context.Context, cfg map[string]string) error {
	_, err := d.db.ExecContext(ctx, `DELETE FROM ng_monitoring_config`)
	if err != nil {
		return err
	}
	for module, data := range cfg {
		_, err = d.db.ExecContext(ctx, "INSERT INTO ng_monitoring_config (module, config) VALUES (?, ?)", module, data)
		if err != nil {
			return err
		}
		log.Info("save config into storage", zap.String("module", module), zap.String("config", data))
	}
	return nil
}

func (d *sqliteDB) LoadConfig(ctx context.Context) (map[string]string, error) {
	res, err := d.db.QueryContext(ctx, `SELECT module, config FROM ng_monitoring_config`)
	if err != nil {
		return nil, err
	}
	if res.Err() != nil {
		return nil, res.Err()
	}
	defer res.Close()
	cfgMap := make(map[string]string)
	for res.Next() {
		var module, config string
		if err := res.Scan(&module, &config); err != nil {
			return nil, err
		}
		cfgMap[module] = config
	}
	return cfgMap, nil
}

func (d *sqliteDB) WriteSQLMeta(ctx context.Context, meta *tipb.SQLMeta) error {
	now := time.Now().Unix()
	_, err := d.writeSQLMetaStmt.ExecContext(ctx, hex.EncodeToString(meta.SqlDigest), meta.NormalizedSql, meta.IsInternalSql, now)
	return err
}

func (d *sqliteDB) QuerySQLMeta(ctx context.Context, digest string) (string, error) {
	r := d.db.QueryRowContext(ctx, `SELECT sql_text FROM sql_digest WHERE digest = ?`, digest)
	var sqlText string
	if err := r.Scan(&sqlText); err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}
		return "", err
	}
	return sqlText, nil
}

func (d *sqliteDB) DeleteSQLMetaBeforeTs(ctx context.Context, ts int64) error {
	_, err := d.db.ExecContext(ctx, `DELETE FROM sql_digest WHERE created_at_ts < ?`, ts)
	return err
}

func (d *sqliteDB) WritePlanMeta(ctx context.Context, meta *tipb.PlanMeta) error {
	now := time.Now().Unix()
	_, err := d.writePlanMetaStmt.ExecContext(ctx, hex.EncodeToString(meta.PlanDigest), meta.NormalizedPlan, meta.EncodedNormalizedPlan, now)
	return err
}

func (d *sqliteDB) QueryPlanMeta(ctx context.Context, digest string) (string, string, error) {
	r := d.db.QueryRowContext(ctx, `SELECT plan_text, encoded_plan FROM plan_digest WHERE digest = ?`, digest)
	var planText, encodedPlan string
	if err := r.Scan(&planText, &encodedPlan); err != nil {
		if err == sql.ErrNoRows {
			return "", "", nil
		}
		return "", "", err
	}
	return planText, encodedPlan, nil
}

func (d *sqliteDB) DeletePlanMetaBeforeTs(ctx context.Context, ts int64) error {
	_, err := d.db.ExecContext(ctx, `DELETE FROM plan_digest WHERE created_at_ts < ?`, ts)
	return err
}

func (d *sqliteDB) ConprofCreateProfileTables(ctx context.Context, id int64) error {
	stmts := []string{
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS conprof_%d_data (ts INTEGER PRIMARY KEY, data BLOB)", id),
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS conprof_%d_meta (ts INTEGER PRIMARY KEY, error TEXT)", id),
	}
	for _, stmt := range stmts {
		if _, err := d.db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func (d *sqliteDB) ConprofDeleteProfileTables(ctx context.Context, id int64) error {
	if _, err := d.db.ExecContext(ctx, "DELETE FROM conprof_targets_meta WHERE id = ?", id); err != nil {
		return err
	}
	if _, err := d.db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS conprof_%d_data", id)); err != nil {
		return err
	}
	if _, err := d.db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS conprof_%d_meta", id)); err != nil {
		return err
	}
	return nil
}

func (d *sqliteDB) ConprofCreateTargetInfo(ctx context.Context, target meta.ProfileTarget, info meta.TargetInfo) error {
	sql := "INSERT INTO conprof_targets_meta (id, kind, component, address, last_scrape_ts) VALUES (?, ?, ?, ?, ?)"
	_, err := d.db.ExecContext(ctx, sql, info.ID, target.Kind, target.Component, target.Address, info.LastScrapeTs)
	return err
}

func (d *sqliteDB) ConprofUpdateTargetInfo(ctx context.Context, info meta.TargetInfo) error {
	_, err := d.db.ExecContext(ctx, "UPDATE conprof_targets_meta set last_scrape_ts = ? where id = ?", info.LastScrapeTs, info.ID)
	return err
}

func (d *sqliteDB) ConprofQueryTargetInfo(ctx context.Context, target meta.ProfileTarget, f func(info meta.TargetInfo) error) error {
	sql := "SELECT id, last_scrape_ts FROM conprof_targets_meta WHERE kind = ? AND component = ? AND address = ?"
	res, err := d.db.QueryContext(ctx, sql, target.Kind, target.Component, target.Address)
	if err != nil {
		return err
	}
	if res.Err() != nil {
		return res.Err()
	}
	defer res.Close()
	for res.Next() {
		info := meta.TargetInfo{}
		err = res.Scan(&info.ID, &info.LastScrapeTs)
		if err != nil {
			return err
		}
		if err := f(info); err != nil {
			return err
		}
	}
	return nil
}

func (d *sqliteDB) ConprofQueryAllProfileTargets(ctx context.Context, f func(target meta.ProfileTarget, info meta.TargetInfo) error) error {
	sql := "SELECT id, kind, component, address, last_scrape_ts FROM conprof_targets_meta"
	res, err := d.db.QueryContext(ctx, sql)
	if err != nil {
		return err
	}
	if res.Err() != nil {
		return res.Err()
	}
	defer res.Close()
	for res.Next() {
		info := meta.TargetInfo{}
		target := meta.ProfileTarget{}
		err = res.Scan(&info.ID, &target.Kind, &target.Component, &target.Address, &info.LastScrapeTs)
		if err != nil {
			return err
		}
		if err := f(target, info); err != nil {
			return err
		}
	}
	return nil
}

func (d *sqliteDB) ConprofWriteProfileData(ctx context.Context, id, ts int64, data []byte) error {
	sql := fmt.Sprintf("INSERT INTO conprof_%d_data (ts, data) VALUES (?, ?)", id)
	_, err := d.db.ExecContext(ctx, sql, ts, data)
	return err
}

func (d *sqliteDB) ConprofQueryProfileData(ctx context.Context, id, begin, end int64, f func(ts int64, data []byte) error) error {
	sql := fmt.Sprintf("SELECT ts, data FROM conprof_%d_data WHERE ts >= ? and ts <= ? ORDER BY ts DESC", id)
	res, err := d.db.QueryContext(ctx, sql, begin, end)
	if err != nil {
		return err
	}
	if res.Err() != nil {
		return res.Err()
	}
	defer res.Close()
	for res.Next() {
		var ts int64
		var data []byte
		err = res.Scan(&ts, &data)
		if err != nil {
			return err
		}
		if err := f(ts, data); err != nil {
			return err
		}
	}
	return nil
}

func (d *sqliteDB) ConprofDeleteProfileDataBeforeTs(ctx context.Context, id, ts int64) error {
	sql := fmt.Sprintf("DELETE FROM conprof_%d_data WHERE ts <= ?", id)
	_, err := d.db.ExecContext(ctx, sql, ts)
	return err
}

func (d *sqliteDB) ConprofWriteProfileMeta(ctx context.Context, id, ts int64, verr string) error {
	sql := fmt.Sprintf("INSERT INTO conprof_%d_meta (ts, error) VALUES (?, ?)", id)
	_, err := d.db.ExecContext(ctx, sql, ts, verr)
	return err
}

func (d *sqliteDB) ConprofQueryProfileMeta(ctx context.Context, id, begin, end int64, f func(ts int64, verr string) error) error {
	sql := fmt.Sprintf("SELECT ts, error FROM conprof_%d_meta WHERE ts >= ? and ts <= ? ORDER BY ts DESC", id)
	res, err := d.db.QueryContext(ctx, sql, begin, end)
	if err != nil {
		return err
	}
	if res.Err() != nil {
		return res.Err()
	}
	defer res.Close()
	for res.Next() {
		var ts int64
		var verr string
		err = res.Scan(&ts, &verr)
		if err != nil {
			return err
		}
		if err := f(ts, verr); err != nil {
			return err
		}
	}
	return nil
}

func (d *sqliteDB) ConprofDeleteProfileMetaBeforeTs(ctx context.Context, id, ts int64) error {
	sql := fmt.Sprintf("DELETE FROM conprof_%d_meta WHERE ts <= ?", id)
	_, err := d.db.ExecContext(ctx, sql, ts)
	return err
}
