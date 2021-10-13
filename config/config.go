package config

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	stdlog "log"
	"path"
	"sort"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/procutil"
	"github.com/pingcap/log"
)

type Config struct {
	Addr    string  `toml:"addr" json:"addr"`
	PD      PD      `toml:"pd" json:"pd"`
	Log     Log     `toml:"log" json:"log"`
	Storage Storage `toml:"storage" json:"storage"`
}

var defaultConfig = Config{
	Addr: ":8428",
	PD: PD{
		Endpoints: nil,
	},
	Log: Log{
		Path:  "log",
		Level: "INFO",
	},
	Storage: Storage{
		Path: "data",
	},
}

func InitConfig(configPath string, override func(config *Config)) (*Config, error) {
	config := defaultConfig

	if len(configPath) > 0 {
		if err := config.Load(configPath); err != nil {
			return nil, err
		}
	}

	override(&config)
	if err := config.valid(); err != nil {
		return nil, err
	}

	return &config, nil
}

func (c *Config) Load(fileName string) error {
	_, err := toml.DecodeFile(fileName, c)
	return err
}

func (c *Config) valid() error {
	var err error

	if len(c.Addr) == 0 {
		return fmt.Errorf("unexpected empty address")
	}

	if err = c.PD.valid(); err != nil {
		return err
	}

	if err = c.Log.valid(); err != nil {
		return err
	}

	if err = c.Storage.valid(); err != nil {
		return err
	}

	return nil
}

type PD struct {
	Endpoints []string `toml:"endpoints" json:"endpoints"`
}

func (p *PD) valid() error {
	if len(p.Endpoints) == 0 {
		return fmt.Errorf("unexpected empty pd endpoints, please specify at least one, e.g. --pd.endpoints \"[127.0.0.1:2379]\"")
	}

	return nil
}

type Storage struct {
	Path string `toml:"path" json:"path"`
}

func (s *Storage) valid() error {
	if len(s.Path) == 0 {
		return fmt.Errorf("unexpected empty storage path")
	}

	return nil
}

type Log struct {
	Path  string `toml:"path" json:"path"`
	Level string `toml:"level" json:"level"`
}

func (l *Log) valid() error {
	if len(l.Path) == 0 {
		return fmt.Errorf("unexpected empty log path")
	}

	if len(l.Level) == 0 {
		return fmt.Errorf("unexpected empty log level")
	}

	if l.Level != "INFO" && l.Level != "WARN" && l.Level != "ERROR" {
		return fmt.Errorf("log level should be INFO, WARN or ERROR")
	}

	return nil
}

func (l *Log) InitDefaultLogger() {
	fileName := path.Join(l.Path, "ng.log")
	logLevel := l.Level

	logger, p, err := log.InitLogger(&log.Config{
		Level: strings.ToLower(logLevel),
		File:  log.FileLogConfig{Filename: fileName},
	})
	if err != nil {
		stdlog.Fatalf("Failed to init logger, err: %v", err)
	}
	log.ReplaceGlobals(logger, p)
}

func ReloadRoutine(ctx context.Context, configPath string, cfg *Config) {
	sighupCh := procutil.NewSighupChan()
	for {
		select {
		case <-ctx.Done():
			return
		case <-sighupCh:
			log.Info("Received SIGHUP and ready to reload config")
		}
		config := new(Config)

		if len(configPath) == 0 {
			log.Warn("Failed to reload config due to empty config path. Please specify the command line argument \"--config <path>\"")
			continue
		}

		if err := config.Load(configPath); err != nil {
			log.Warn("Failed to reload config", zap.Error(err))
			continue
		}

		if len(config.PD.Endpoints) == 0 {
			log.Warn("Unexpected empty PD endpoints")
			continue
		}

		if configsEqual(cfg, config) {
			log.Info("PD endpoints changed", zap.Strings("endpoints", config.PD.Endpoints))
		}

		// TODO: apply config change
		cfg = config
	}
}

func configsEqual(a, b *Config) bool {
	sort.Strings(a.PD.Endpoints)
	sort.Strings(b.PD.Endpoints)

	if len(a.PD.Endpoints) != len(b.PD.Endpoints) {
		return false
	}
	for i := range a.PD.Endpoints {
		if a.PD.Endpoints[i] != b.PD.Endpoints[i] {
			return false
		}
	}

	return true
}
