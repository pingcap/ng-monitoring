package main

import (
	"flag"

	"github.com/zhongzc/diag_backend/service"
	"github.com/zhongzc/diag_backend/storage"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/procutil"
	"github.com/pingcap/log"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
)

var (
	listenAddr = pflag.String("listen-addr", ":8428", "TCP address to listen for http/grpc connections")

	// tsdbPath is a path to storage timeseries data.
	tsdbPath            = pflag.String("tsdb-path", "tsdb-data", "Path to storage tsdb data")
	tsdbLogOutput       = pflag.String("tsdb-log-output", "stderr", "Output for the logs. Supported values: stderr, stdout")
	tsdbRetentionPeriod = pflag.String("tsdb-retention-period", "1", "Data with timestamps outside the retentionPeriod is automatically deleted\nThe following optional suffixes are supported: h (hour), d (day), w (week), y (year). If suffix isn't set, then the duration is counted in months")

	// docdbPath is a path to storage document data, i.e. sql digest and plan digest.
	docdbPath = pflag.String("docdb-path", "docdb-data", "Path to storage document data")
)

func main() {
	pflag.Parse()

	// config victoria metrics arguments
	_ = flag.Set("storageDataPath", *tsdbPath)
	_ = flag.Set("retentionPeriod", *tsdbRetentionPeriod)
	_ = flag.Set("loggerOutput", *tsdbLogOutput)
	_ = flag.CommandLine.Parse(nil)

	storage.Init(*tsdbPath, *docdbPath)
	defer storage.Stop()

	service.Init(*listenAddr)
	defer service.Stop()

	sig := procutil.WaitForSigterm()
	log.Info("received signal", zap.String("sig", sig.String()))
}
