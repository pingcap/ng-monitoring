package docdb

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/pingcap/ng-monitoring/component/conprof/meta"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func testDocDB(t *testing.T, db DocDB) {
	ctx := context.Background()
	if deadline, ok := t.Deadline(); ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()

	err := db.SaveConfig(ctx, map[string]string{"test_k": "test_v"})
	require.NoError(t, err)

	cfgs, err := db.LoadConfig(ctx)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"test_k": "test_v"}, cfgs)

	err = db.WriteSQLMeta(ctx, &tipb.SQLMeta{
		SqlDigest:     []byte("test_digest"),
		NormalizedSql: "test_sql",
		IsInternalSql: true,
	})
	require.NoError(t, err)
	err = db.WritePlanMeta(ctx, &tipb.PlanMeta{
		PlanDigest:            []byte("test_digest"),
		NormalizedPlan:        "test_plan",
		EncodedNormalizedPlan: "test_encoded_plan",
	})
	require.NoError(t, err)

	sqlDigest, err := db.QuerySQLMeta(ctx, hex.EncodeToString([]byte("test_digest")))
	require.NoError(t, err)
	require.Equal(t, "test_sql", sqlDigest)
	planDigest, encodedPlan, err := db.QueryPlanMeta(ctx, hex.EncodeToString([]byte("test_digest")))
	require.NoError(t, err)
	require.Equal(t, "test_plan", planDigest)
	require.Equal(t, "test_encoded_plan", encodedPlan)

	// genjiDB does not support DeleteSQLMetaBeforeTs and DeletePlanMetaBeforeTs
	if _, ok := db.(*genjiDB); !ok {
		safePointTs := time.Now().Unix() + 100
		err = db.DeleteSQLMetaBeforeTs(ctx, safePointTs)
		require.NoError(t, err)
		err = db.DeletePlanMetaBeforeTs(ctx, safePointTs)
		require.NoError(t, err)
		sqlDigest, err = db.QuerySQLMeta(ctx, hex.EncodeToString([]byte("test_digest")))
		require.NoError(t, err)
		require.Equal(t, "", sqlDigest)
		planDigest, encodedPlan, err = db.QueryPlanMeta(ctx, hex.EncodeToString([]byte("test_digest")))
		require.NoError(t, err)
		require.Equal(t, "", planDigest)
		require.Equal(t, "", encodedPlan)
	}

	err = db.ConprofCreateProfileTables(ctx, 1)
	require.NoError(t, err)

	err = db.ConprofCreateTargetInfo(ctx, meta.ProfileTarget{
		Kind:      "test_kind",
		Component: "test_component",
		Address:   "test_address",
	}, meta.TargetInfo{
		ID:           1,
		LastScrapeTs: 2,
	})
	require.NoError(t, err)

	targets := []meta.ProfileTarget{}
	infos := []meta.TargetInfo{}
	db.ConprofQueryAllProfileTargets(ctx, func(target meta.ProfileTarget, info meta.TargetInfo) error {
		targets = append(targets, target)
		infos = append(infos, info)
		return nil
	})
	require.Len(t, targets, 1)
	require.Len(t, infos, 1)
	require.Equal(t, meta.ProfileTarget{
		Kind:      "test_kind",
		Component: "test_component",
		Address:   "test_address",
	}, targets[0])
	require.Equal(t, meta.TargetInfo{
		ID:           1,
		LastScrapeTs: 2,
	}, infos[0])

	infos = []meta.TargetInfo{}
	db.ConprofQueryTargetInfo(ctx, meta.ProfileTarget{
		Kind:      "test_kind",
		Component: "test_component",
		Address:   "test_address",
	}, func(info meta.TargetInfo) error {
		infos = append(infos, info)
		return nil
	})
	require.Len(t, infos, 1)
	require.Equal(t, meta.TargetInfo{
		ID:           1,
		LastScrapeTs: 2,
	}, infos[0])

	err = db.ConprofUpdateTargetInfo(ctx, meta.TargetInfo{
		ID:           1,
		LastScrapeTs: 3,
	})
	require.NoError(t, err)

	infos = []meta.TargetInfo{}
	db.ConprofQueryTargetInfo(ctx, meta.ProfileTarget{
		Kind:      "test_kind",
		Component: "test_component",
		Address:   "test_address",
	}, func(info meta.TargetInfo) error {
		infos = append(infos, info)
		return nil
	})
	require.Len(t, infos, 1)
	require.Equal(t, meta.TargetInfo{
		ID:           1,
		LastScrapeTs: 3,
	}, infos[0])

	err = db.ConprofWriteProfileData(ctx, 1, 2, []byte("test_data"))
	require.NoError(t, err)

	tss := []int64{}
	datas := [][]byte{}
	err = db.ConprofQueryProfileData(ctx, 1, 1, 3, func(ts int64, data []byte) error {
		tss = append(tss, ts)
		datas = append(datas, data)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, tss, 1)
	require.Len(t, datas, 1)
	require.Equal(t, int64(2), tss[0])
	require.Equal(t, []byte("test_data"), datas[0])

	err = db.ConprofDeleteProfileDataBeforeTs(ctx, 1, 4)
	require.NoError(t, err)

	tss = []int64{}
	datas = [][]byte{}
	err = db.ConprofQueryProfileData(ctx, 1, 1, 3, func(ts int64, data []byte) error {
		tss = append(tss, ts)
		datas = append(datas, data)
		return nil
	})
	require.NoError(t, err)
	require.Empty(t, tss)
	require.Empty(t, datas)

	err = db.ConprofWriteProfileMeta(ctx, 1, 2, "test_err")
	require.NoError(t, err)

	tss = []int64{}
	verrs := []string{}
	err = db.ConprofQueryProfileMeta(ctx, 1, 1, 3, func(ts int64, verr string) error {
		tss = append(tss, ts)
		verrs = append(verrs, verr)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, tss, 1)
	require.Len(t, verrs, 1)
	require.Equal(t, int64(2), tss[0])
	require.Equal(t, "test_err", verrs[0])

	err = db.ConprofDeleteProfileMetaBeforeTs(ctx, 1, 4)
	require.NoError(t, err)

	tss = []int64{}
	verrs = []string{}
	err = db.ConprofQueryProfileMeta(ctx, 1, 1, 3, func(ts int64, verr string) error {
		tss = append(tss, ts)
		verrs = append(verrs, verr)
		return nil
	})
	require.NoError(t, err)
	require.Empty(t, tss)
	require.Empty(t, verrs)

	err = db.ConprofDeleteProfileTables(ctx, 1)
	require.NoError(t, err)

	targets = []meta.ProfileTarget{}
	infos = []meta.TargetInfo{}
	db.ConprofQueryAllProfileTargets(ctx, func(target meta.ProfileTarget, info meta.TargetInfo) error {
		targets = append(targets, target)
		infos = append(infos, info)
		return nil
	})
	require.Empty(t, targets)
	require.Empty(t, infos)
}
