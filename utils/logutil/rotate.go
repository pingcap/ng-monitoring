package logutil

import (
	"fmt"

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
	return &lumberjack.Logger{
		Filename:   cfg.Filename,
		MaxSize:    cfg.MaxSize,
		MaxBackups: cfg.MaxBackups,
		MaxAge:     cfg.MaxDays,
		LocalTime:  true,
	}, nil
}
