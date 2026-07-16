package docdb

import (
	"io"
	stdlog "log"
	"os"
	"path"

	"github.com/pingcap/log"
	"github.com/pingcap/ng-monitoring/utils/logutil"
	"go.uber.org/zap"
)

type loggingLevel int

const (
	DEBUG loggingLevel = iota
	INFO
	WARN
	ERROR
)

const (
	LevelDebug = "DEBUG"
	LevelInfo  = "INFO"
	LevelWarn  = "WARN"
	LevelError = "ERROR"
)

type logger struct {
	*stdlog.Logger
	level loggingLevel
}

func initLogger(logPath, logLevel string, fileCfg log.FileLogConfig) (*logger, error) {
	var err error
	var logDir string
	if logPath != "" {
		logDir = logPath
	} else {
		logDir = path.Join(logPath, "docdb-log")
		err := os.MkdirAll(logDir, os.ModePerm)
		if err != nil {
			return nil, err
		}
	}
	logFileName := path.Join(logDir, "docdb.log")
	fileCfg.Filename = logFileName
	logFile, err := logutil.NewRotateWriter(fileCfg)
	if err != nil {
		// Need to log via the default logger due to `l` is not initialized.
		log.Warn("Failed to init logger", zap.String("filename", logFileName))
		return nil, err
	}
	var level loggingLevel
	switch logLevel {
	case LevelDebug:
		level = DEBUG
	case LevelInfo:
		level = INFO
	case LevelWarn:
		level = WARN
	case LevelError:
		level = ERROR
	default:
		log.Fatal("Unsupported log level", zap.String("level", logLevel))
	}
	return &logger{Logger: stdlog.New(io.Writer(logFile), "badger ", stdlog.LstdFlags), level: level}, nil
}

func (l *logger) Errorf(f string, v ...interface{}) {
	if l.level <= ERROR {
		l.Printf("ERROR: "+f, v...)
	}
}

func (l *logger) Warningf(f string, v ...interface{}) {
	if l.level <= WARN {
		l.Printf("WARN: "+f, v...)
	}
}

func (l *logger) Infof(f string, v ...interface{}) {
	if l.level <= INFO {
		l.Printf("INFO: "+f, v...)
	}
}

func (l *logger) Debugf(f string, v ...interface{}) {
	if l.level <= DEBUG {
		l.Printf("DEBUG: "+f, v...)
	}
}
