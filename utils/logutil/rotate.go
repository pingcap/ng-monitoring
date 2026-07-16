package logutil

import (
	"fmt"
	"os"

	"github.com/pingcap/log"
	"gopkg.in/natefinch/lumberjack.v2"
)

const defaultMaxSizeMB = 300

func NewRotateWriter(cfg log.FileLogConfig) (*lumberjack.Logger, error) {
	if cfg.Filename == "" {
		return nil, fmt.Errorf("unexpected empty log filename")
	}
	if cfg.MaxSize == 0 {
		cfg.MaxSize = defaultMaxSizeMB
	}
	file, err := os.OpenFile(cfg.Filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	if err := file.Close(); err != nil {
		return nil, err
	}
	return &lumberjack.Logger{
		Filename:   cfg.Filename,
		MaxSize:    cfg.MaxSize,
		MaxBackups: cfg.MaxBackups,
		MaxAge:     cfg.MaxDays,
		LocalTime:  true,
	}, nil
}
