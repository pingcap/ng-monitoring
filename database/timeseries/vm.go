package timeseries

import (
	"flag"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/pingcap/ng-monitoring/config"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/promql"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

func Init(cfg *config.Config) {
	if err := initLogger(cfg); err != nil {
		log.Fatal("Failed to open log file", zap.Error(err))
	}
	initDataDir(path.Join(cfg.Storage.Path, "tsdb"))

	_ = flag.Set("retentionPeriod", cfg.TSDB.RetentionPeriod)
	_ = flag.Set("search.maxStepForPointsAdjustment", "1s")
	_ = flag.Set("search.maxUniqueTimeseries", fmt.Sprintf("%d", cfg.TSDB.SearchMaxUniqueTimeseries))

	// Some components in VictoriaMetrics want parsed arguments, i.e. assert `flag.Parsed()`. Make them happy.
	_ = flag.CommandLine.Parse(nil)

	startTime := time.Now()
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

func initLogger(cfg *config.Config) error {
	_ = flag.Set("loggerOutput", "stderr")
	_ = flag.Set("loggerLevel", mapLogLevel(cfg.Log.Level))

	var logDir string
	if cfg.Log.Path != "" {
		logDir = cfg.Log.Path
	} else {
		// create tsdb log dir
		logDir = path.Join(cfg.Storage.Path, "tsdb-log")
		err := os.MkdirAll(logDir, os.ModePerm)
		if err != nil {
			return err
		}
	}

	// VictoriaMetrics only supports stdout or stderr as log output.
	// To output the log to the specified file, redirect stderr to that file.
	logFileName := path.Join(logDir, "tsdb.log")
	file, err := os.OpenFile(logFileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	if err = dup2(int(file.Fd()), int(os.Stderr.Fd())); err != nil {
		return err
	}
	logger.Init()

	return nil
}

func initDataDir(dataPath string) {
	_ = flag.Set("storageDataPath", dataPath)
}

func mapLogLevel(level string) string {
	switch level {
	case config.LevelDebug, config.LevelInfo:
		return "INFO"
	case config.LevelWarn:
		return "WARN"
	case config.LevelError:
		return "ERROR"
	default:
		return "INFO"
	}
}
