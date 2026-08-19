package timeseries

import (
	"flag"
	"testing"

	"github.com/pingcap/ng-monitoring/config"

	// Registers the search.maxQueryDuration flag so the wiring below can be verified.
	_ "github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect"
	"github.com/stretchr/testify/require"
)

func TestSetVMFlagsSearchMaxQueryDuration(t *testing.T) {
	f := flag.Lookup("search.maxQueryDuration")
	require.NotNil(t, f, "VictoriaMetrics must register search.maxQueryDuration")
	defaultValue := f.Value.String()
	require.Equal(t, "30s", defaultValue)

	// Unset preserves the VictoriaMetrics default.
	cfg := &config.Config{}
	setVMFlags(cfg)
	require.Equal(t, defaultValue, flag.Lookup("search.maxQueryDuration").Value.String())

	// A configured value is applied to the VM flag.
	cfg.TSDB.SearchMaxQueryDuration = "5m"
	setVMFlags(cfg)
	require.Equal(t, "5m0s", flag.Lookup("search.maxQueryDuration").Value.String())
}
