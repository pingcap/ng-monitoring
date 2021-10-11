package document

import (
	"log"
	"os"
)

type loggingLevel int

const (
	DEBUG loggingLevel = iota
	INFO
	WARN
	ERROR
)

type logger struct {
	*log.Logger
	level loggingLevel
}

func simpleLogger(logFile string, logLevel string) (*logger, error) {
	file, err := os.OpenFile(logFile, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	var level loggingLevel
	switch logLevel {
	case "DEBUG":
		level = DEBUG
	case "INFO":
		level = INFO
	case "WARN":
		level = WARN
	case "ERROR":
		level = ERROR
	default:
		log.Fatal("Unsupported log level", logLevel)
	}

	return &logger{Logger: log.New(file, "badger ", log.LstdFlags), level: level}, nil
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
