package timeseries

import (
	"flag"
	"os"
	"syscall"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/promql"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/pingcap/log"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
)

var (
	retentionPeriod = pflag.String("retention-period", "1", "Data with timestamps outside the retentionPeriod is automatically deleted\nThe following optional suffixes are supported: h (hour), d (day), w (week), y (year). If suffix isn't set, then the duration is counted in months")
)

func Init(logFileName string, logLevel string, dataPath string) {
	if err := initLogger(logFileName, logLevel); err != nil {
		log.Fatal("Failed to open log file", zap.Error(err))
	}
	initDataDir(dataPath)

	_ = flag.Set("retentionPeriod", *retentionPeriod)

	// Some components in VictoriaMetrics want parsed arguments, i.e. assert `flag.Parsed()`. Make them happy.
	_ = flag.CommandLine.Parse(nil)

	startTime := time.Now()
	storage.SetMinScrapeIntervalForDeduplication(0)
	vmstorage.Init(promql.ResetRollupResultCacheIfNeeded)
	vmselect.Init()
	vminsert.Init()

	logger.Infof("started VictoriaMetrics in %.3f seconds", time.Since(startTime).Seconds())
}

func Stop() {
	startTime := time.Now()
	vminsert.Stop()
	logger.Infof("successfully shut down the webservice in %.3f seconds", time.Since(startTime).Seconds())

	vmstorage.Stop()
	vmselect.Stop()

	fs.MustStopDirRemover()

	logger.Infof("the VictoriaMetrics has been stopped in %.3f seconds", time.Since(startTime).Seconds())
}

func initLogger(logPath string, level string) error {
	_ = flag.Set("loggerOutput", "stderr")
	_ = flag.Set("loggerLevel", level)

	// VictoriaMetrics only supports stdout or stderr as log output.
	// To output the log to the specified file, redirect stderr to that file.
	file, err := os.OpenFile(logPath, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}

	if err = syscall.Dup2(int(file.Fd()), int(os.Stderr.Fd())); err != nil {
		return err
	}
	if err = syscall.Close(int(file.Fd())); err != nil {
		return err
	}
	logger.Init()

	return nil
}

func initDataDir(dataPath string) {
	_ = flag.Set("storageDataPath", dataPath)
}
