package document

import (
	stdlog "log"
	"os"
	"path"

	"github.com/zhongzc/ng_monitoring/config"

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

func simpleLogger(l *config.Log) (*logger, error) {
	logFileName := path.Join(l.Path, "docdb.log")
	file, err := os.OpenFile(logFileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		// Need to log via the default logger due to `l` is not initialized.
		log.Warn("Failed to init logger", zap.String("filename", logFileName))
		return nil, err
	}

	var level loggingLevel
	switch l.Level {
	case "INFO":
		level = INFO
	case "WARN":
		level = WARN
	case "ERROR":
		level = ERROR
	default:
		log.Fatal("Unsupported log level", zap.String("level", l.Level))
	}

	return &logger{Logger: stdlog.New(file, "badger ", stdlog.LstdFlags), level: level}, nil
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
