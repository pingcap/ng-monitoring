package plan_test

import (
	"os"
	"testing"

	"github.com/pingcap/ng-monitoring/component/topsql/codec/plan"

	"github.com/stretchr/testify/require"
)

func TestBigPlan(t *testing.T) {
	t.Parallel()

	bigEncodedPlan, err := os.ReadFile("testdata/big_encoded_plan.txt")
	require.NoError(t, err)

	planText, err := plan.Decode(string(bigEncodedPlan))
	require.NoError(t, err)

	bigDecodedPlan, err := os.ReadFile("testdata/big_decoded_plan.txt")
	require.NoError(t, err)
	require.Equal(t, planText, string(bigDecodedPlan))
}

func TestSmallPlan(t *testing.T) {
	t.Parallel()

	encodedPlan := "WrAwCTM4CTAJdGFibGU6R0xPQkFMX1ZBUklBQkxFUywgaW5kZXg6UFJJTUFSWSgRGZBfTkFNRSksIGtlZXAgb3JkZXI6ZmFsc2UsIGRlc2M6ZmFsc2UK"
	expectedPlanText := "\tBatch_Point_Get\troot\ttable:GLOBAL_VARIABLES, index:PRIMARY(VARIABLE_NAME), keep order:false, desc:false"

	planText, err := plan.Decode(encodedPlan)
	require.NoError(t, err)
	require.Equal(t, planText, expectedPlanText)
}
