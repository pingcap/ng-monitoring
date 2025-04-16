package docdb

import (
	"context"
	"io"

	"github.com/pingcap/ng-monitoring/component/conprof/meta"
	"github.com/pingcap/tipb/go-tipb"
)

type DocDB interface {
	io.Closer

	SaveConfig(ctx context.Context, cfg map[string]string) error
	LoadConfig(ctx context.Context) (map[string]string, error)

	WriteSQLMeta(ctx context.Context, meta *tipb.SQLMeta) error
	QuerySQLMeta(ctx context.Context, digest string) (string, error)
	DeleteSQLMetaBeforeTs(ctx context.Context, ts int64) error
	WritePlanMeta(ctx context.Context, meta *tipb.PlanMeta) error
	QueryPlanMeta(ctx context.Context, digest string) (string, string, error)
	DeletePlanMetaBeforeTs(ctx context.Context, ts int64) error

	ConprofCreateProfileTables(ctx context.Context, id int64) error
	ConprofDeleteProfileTables(ctx context.Context, id int64) error
	ConprofCreateTargetInfo(ctx context.Context, target meta.ProfileTarget, info meta.TargetInfo) error
	ConprofUpdateTargetInfo(ctx context.Context, info meta.TargetInfo) error
	ConprofQueryTargetInfo(ctx context.Context, target meta.ProfileTarget, f func(info meta.TargetInfo) error) error
	ConprofQueryAllProfileTargets(ctx context.Context, f func(target meta.ProfileTarget, info meta.TargetInfo) error) error
	ConprofWriteProfileData(ctx context.Context, id, ts int64, data []byte) error
	ConprofQueryProfileData(ctx context.Context, id, begin, end int64, f func(ts int64, data []byte) error) error
	ConprofDeleteProfileDataBeforeTs(ctx context.Context, id, ts int64) error
	ConprofWriteProfileMeta(ctx context.Context, id, ts int64, err string) error
	ConprofQueryProfileMeta(ctx context.Context, id, begin, end int64, f func(ts int64, verr string) error) error
	ConprofDeleteProfileMetaBeforeTs(ctx context.Context, id, ts int64) error
}
