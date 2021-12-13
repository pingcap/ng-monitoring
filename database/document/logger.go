package document

import (
	stdlog "log"
	"os"
	"path"

	"github.com/pingcap/ng-monitoring/config"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type loggingLevel int

const (
	DEBUG loggingLevel = iota
	INFO
	WARN
	ERROR
)

type logger struct {
	*stdlog.Logger
	level loggingLevel
}

func initLogger(cfg *config.Config) (*logger, error) {
	var err error
	var logDir string
	if cfg.Log.Path != "" {
		logDir = cfg.Log.Path
	} else {
		logDir = path.Join(cfg.Storage.Path, "docdb-log")
		err := os.MkdirAll(logDir, os.ModePerm)
		if err != nil {
			return nil, err
		}
	}

	logFileName := path.Join(logDir, "docdb.log")
	logFile, err := os.OpenFile(logFileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		// Need to log via the default logger due to `l` is not initialized.
		log.Warn("Failed to init logger", zap.String("filename", logFileName))
		return nil, err
	}

	var level loggingLevel
	switch cfg.Log.Level {
	case config.LevelDebug:
		level = DEBUG
	case config.LevelInfo:
		level = INFO
	case config.LevelWarn:
		level = WARN
	case config.LevelError:
		level = ERROR
	default:
		log.Fatal("Unsupported log level", zap.String("level", cfg.Log.Level))
	}

	return &logger{Logger: stdlog.New(logFile, "badger ", stdlog.LstdFlags), level: level}, nil
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
