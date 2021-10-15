package config

import (
	"context"
	"crypto/tls"
	"fmt"
	stdlog "log"
	"path"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/procutil"
	"github.com/pingcap/log"
	commonconfig "github.com/prometheus/common/config"
	"go.etcd.io/etcd/pkg/transport"
	"go.uber.org/zap"
)

const (
	DefProfilingEnable               = true
	DefProfilingIntervalSeconds      = 10
	DefProfileSeconds                = 5
	DefProfilingTimeoutSeconds       = 120
	DefProfilingDataRetentionSeconds = 3 * 24 * 60 * 60 // 3 days
)

type Config struct {
	Addr              string                  `toml:"addr" json:"addr"`
	AdvertiseAddress  string                  `toml:"advertise_address" json:"advertise_address"`
	PD                PD                      `toml:"pd" json:"pd"`
	Log               Log                     `toml:"log" json:"log"`
	Storage           Storage                 `toml:"storage" json:"storage"`
	ContinueProfiling ContinueProfilingConfig `toml:"-" json:"continuous_profiling"`
	Security          Security                `toml:"security" json:"security"`
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
	ContinueProfiling: ContinueProfilingConfig{
		Enable:               DefProfilingEnable,
		ProfileSeconds:       DefProfileSeconds,
		IntervalSeconds:      DefProfilingIntervalSeconds,
		TimeoutSeconds:       DefProfilingTimeoutSeconds,
		DataRetentionSeconds: DefProfilingDataRetentionSeconds,
	},
}

var globalConf atomic.Value

func GetGlobalConfig() *Config {
	return globalConf.Load().(*Config)
}

// StoreGlobalConfig stores a new config to the globalConf. It mostly uses in the test to avoid some data races.
func StoreGlobalConfig(config *Config) {
	globalConf.Store(config)
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
	StoreGlobalConfig(&config)
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

		if !configsEqual(cfg, config) {
			log.Info("PD endpoints changed", zap.Strings("endpoints", config.PD.Endpoints))
		}

		// TODO: apply config change
		cfg = config
		StoreGlobalConfig(config)
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

func (c *Config) GetHTTPScheme() string {
	if c.Security.GetTLSConfig() != nil {
		return "https"
	}
	return "http"
}

type Security struct {
	SSLCA     string      `toml:"ssl_ca" json:"ssl_ca"`
	SSLCert   string      `toml:"ssl_cert" json:"ssl_cert"`
	SSLKey    string      `toml:"ssl_key" json:"ssl_key"`
	tlsConfig *tls.Config `toml:"-" json:"-"`
}

func (s *Security) GetTLSConfig() *tls.Config {
	if s.tlsConfig != nil {
		return s.tlsConfig
	}
	if s.SSLCA == "" || s.SSLCert == "" || s.SSLKey == "" {
		return nil
	}
	s.tlsConfig = buildTLSConfig(s.SSLCA, s.SSLKey, s.SSLCert)
	return s.tlsConfig
}

func (s *Security) GetHTTPClientConfig() commonconfig.HTTPClientConfig {
	return commonconfig.HTTPClientConfig{
		TLSConfig: commonconfig.TLSConfig{
			CAFile:   s.SSLCA,
			CertFile: s.SSLCert,
			KeyFile:  s.SSLKey,
		},
	}
}

func buildTLSConfig(caPath, keyPath, certPath string) *tls.Config {
	tlsInfo := transport.TLSInfo{
		TrustedCAFile: caPath,
		KeyFile:       keyPath,
		CertFile:      certPath,
	}
	tlsConfig, err := tlsInfo.ClientConfig()
	if err != nil {
		log.Fatal("Failed to load certificates", zap.Error(err))
	}
	return tlsConfig
}

type ContinueProfilingConfig struct {
	Enable               bool `json:"enable"`
	ProfileSeconds       int  `json:"profile_seconds"`
	IntervalSeconds      int  `json:"interval_seconds"`
	TimeoutSeconds       int  `json:"timeout_seconds"`
	DataRetentionSeconds int  `json:"data_retention_seconds"`
}

// ScrapeConfig configures a scraping unit for conprof.
type ScrapeConfig struct {
	ComponentName string `yaml:"component_name,omitempty"`
	// How frequently to scrape the targets of this scrape config.
	ScrapeInterval time.Duration `yaml:"scrape_interval,omitempty"`
	// The timeout for scraping targets of this config.
	ScrapeTimeout time.Duration `yaml:"scrape_timeout,omitempty"`

	ProfilingConfig *ProfilingConfig `yaml:"profiling_config,omitempty"`
	Targets         []string         `yaml:"targets"`
}

type ProfilingConfig struct {
	PprofConfig PprofConfig `yaml:"pprof_config,omitempty"`
}

type PprofConfig map[string]*PprofProfilingConfig

type PprofProfilingConfig struct {
	Path    string            `yaml:"path,omitempty"`
	Seconds int               `yaml:"seconds"`
	Header  map[string]string `yaml:"header,omitempty"`
	Params  map[string]string `yaml:"params,omitempty"`
}
