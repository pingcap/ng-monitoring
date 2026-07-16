package timeseries

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
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

var (
	tsdbLogForwarderMu sync.Mutex
	tsdbLogForwarder   *stderrForwarder
	originalStderr     *os.File
)

type stderrForwarder struct {
	reader   *os.File
	sink     io.WriteCloser
	copyDone chan error
}

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
	stopTSDBLogForwarder()
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
	logFileName := path.Join(logDir, "tsdb.log")
	if err := replaceTSDBLogForwarder(cfg.Log.FileLogConfig(logFileName)); err != nil {
		return err
	}
	logger.Init()

	return nil
}

func replaceTSDBLogForwarder(fileCfg log.FileLogConfig) error {
	tsdbLogForwarderMu.Lock()
	defer tsdbLogForwarderMu.Unlock()

	stopTSDBLogForwarderLocked()
	if err := ensureOriginalStderrLocked(); err != nil {
		return err
	}
	keepOriginalStderr := false
	defer func() {
		if !keepOriginalStderr {
			closeOriginalStderrLocked()
		}
	}()

	sink, err := logutil.NewRotateWriter(fileCfg)
	if err != nil {
		return err
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		_ = sink.Close()
		return err
	}
	if err = dup2(int(writer.Fd()), int(os.Stderr.Fd())); err != nil {
		_ = reader.Close()
		_ = writer.Close()
		_ = sink.Close()
		return err
	}
	_ = writer.Close()

	forwarder := &stderrForwarder{
		reader:   reader,
		sink:     sink,
		copyDone: make(chan error, 1),
	}
	go forwarder.forward(originalStderr)
	tsdbLogForwarder = forwarder
	keepOriginalStderr = true
	return nil
}

func stopTSDBLogForwarder() {
	tsdbLogForwarderMu.Lock()
	defer tsdbLogForwarderMu.Unlock()
	stopTSDBLogForwarderLocked()
}

func stopTSDBLogForwarderLocked() {
	if tsdbLogForwarder == nil {
		closeOriginalStderrLocked()
		return
	}

	forwarder := tsdbLogForwarder
	tsdbLogForwarder = nil

	if originalStderr == nil {
		_ = forwarder.reader.Close()
	} else {
		restoreErr := dup2(int(originalStderr.Fd()), int(os.Stderr.Fd()))
		if restoreErr != nil {
			log.Warn("failed to restore stderr after tsdb logging", zap.Error(restoreErr))
			_ = forwarder.reader.Close()
		}
	}

	if copyErr := <-forwarder.copyDone; copyErr != nil {
		log.Warn("tsdb log forwarder exited unexpectedly", zap.Error(copyErr))
	}
	if err := forwarder.reader.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
		log.Warn("failed to close tsdb log reader", zap.Error(err))
	}
	if err := forwarder.sink.Close(); err != nil {
		log.Warn("failed to close tsdb log writer", zap.Error(err))
	}
	closeOriginalStderrLocked()
}

func ensureOriginalStderrLocked() error {
	if originalStderr != nil {
		return nil
	}
	fd, err := dup(int(os.Stderr.Fd()))
	if err != nil {
		return err
	}
	originalStderr = os.NewFile(uintptr(fd), "original-stderr")
	return nil
}

func closeOriginalStderrLocked() {
	if originalStderr == nil {
		return
	}
	if err := originalStderr.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
		log.Warn("failed to close saved stderr", zap.Error(err))
	}
	originalStderr = nil
}

func (f *stderrForwarder) forward(fallback io.Writer) {
	f.copyDone <- copyWithFallback(f.reader, f.sink, fallback)
}

func copyWithFallback(reader io.Reader, primary io.Writer, fallback io.Writer) error {
	buf := make([]byte, 32*1024)
	writer := primary
	switchedToFallback := false

	for {
		n, err := reader.Read(buf)
		if n > 0 {
			if _, writeErr := writer.Write(buf[:n]); writeErr != nil {
				if !switchedToFallback && fallback != nil {
					switchedToFallback = true
					writer = fallback
					_, _ = fmt.Fprintf(fallback, "tsdb log rotation writer failed, fallback to stderr: %v\n", writeErr)
					_, _ = writer.Write(buf[:n])
				} else {
					return writeErr
				}
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
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
