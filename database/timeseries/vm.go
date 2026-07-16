package timeseries

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"time"

	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/ng-monitoring/utils/logutil"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/promql"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"go.uber.org/zap"

	"github.com/pingcap/log"
)

var tsdbLogForwarderStarted bool

func Init(cfg *config.Config) {
	if err := initLogger(cfg); err != nil {
		log.Fatal("Failed to open log file", zap.Error(err))
	}
	initDataDir(path.Join(cfg.Storage.Path, "tsdb"))

	_ = flag.Set("retentionPeriod", cfg.TSDB.RetentionPeriod)
	_ = flag.Set("search.maxStepForPointsAdjustment", "1s")
	_ = flag.Set("search.maxUniqueTimeseries", fmt.Sprintf("%d", cfg.TSDB.SearchMaxUniqueTimeseries))
	if cfg.TSDB.MemoryAllowedBytes > 0 {
		_ = flag.Set("memory.allowedBytes", fmt.Sprintf("%d", cfg.TSDB.MemoryAllowedBytes))
	}
	if cfg.TSDB.MemoryAllowedPercent > 0 {
		_ = flag.Set("memory.allowedPercent", fmt.Sprintf("%f", cfg.TSDB.MemoryAllowedPercent))
	}
	if cfg.TSDB.CacheSizeIndexDBDataBlocks != "" {
		_ = flag.Set("storage.cacheSizeIndexDBDataBlocks", cfg.TSDB.CacheSizeIndexDBDataBlocks)
	}
	if cfg.TSDB.CacheSizeIndexDBDataBlocksSparse != "" {
		_ = flag.Set("storage.cacheSizeIndexDBDataBlocksSparse", cfg.TSDB.CacheSizeIndexDBDataBlocksSparse)
	}
	if cfg.TSDB.CacheSizeIndexDBIndexBlocks != "" {
		_ = flag.Set("storage.cacheSizeIndexDBIndexBlocks", cfg.TSDB.CacheSizeIndexDBIndexBlocks)
	}
	if cfg.TSDB.CacheSizeIndexDBTagFilters != "" {
		_ = flag.Set("storage.cacheSizeIndexDBTagFilters", cfg.TSDB.CacheSizeIndexDBTagFilters)
	}
	if cfg.TSDB.CacheSizeMetricNamesStats != "" {
		_ = flag.Set("storage.cacheSizeMetricNamesStats", cfg.TSDB.CacheSizeMetricNamesStats)
	}
	if cfg.TSDB.CacheSizeStorageTSID != "" {
		_ = flag.Set("storage.cacheSizeStorageTSID", cfg.TSDB.CacheSizeStorageTSID)
	}

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
	// Redirect stderr to a pipe and forward it to a rotating file writer.
	if !tsdbLogForwarderStarted {
		logFileName := path.Join(logDir, "tsdb.log")
		file, err := logutil.NewRotateWriter(cfg.Log.FileLogConfig(logFileName))
		if err != nil {
			return err
		}
		reader, writer, err := os.Pipe()
		if err != nil {
			return err
		}
		go func() {
			_, _ = io.Copy(file, reader)
		}()
		if err = dup2(int(writer.Fd()), int(os.Stderr.Fd())); err != nil {
			_ = reader.Close()
			_ = writer.Close()
			return err
		}
		_ = writer.Close()
		tsdbLogForwarderStarted = true
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
