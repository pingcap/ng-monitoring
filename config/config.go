package config

import (
	"context"
	"crypto/tls"
	"fmt"
	stdlog "log"
	"net"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/ng-monitoring/database/docdb"
	"github.com/pingcap/ng-monitoring/utils"

	"github.com/BurntSushi/toml"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/procutil"
	"github.com/pingcap/log"
	commonconfig "github.com/prometheus/common/config"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	"go.uber.org/zap"
)

type Config struct {
	Address           string                  `toml:"address" json:"address"`
	AdvertiseAddress  string                  `toml:"advertise-address" json:"advertise_address"`
	Go                Go                      `toml:"go" json:"go"`
	PD                PD                      `toml:"pd" json:"pd"`
	Log               Log                     `toml:"log" json:"log"`
	Storage           Storage                 `toml:"storage" json:"storage"`
	ContinueProfiling ContinueProfilingConfig `toml:"-" json:"continuous_profiling"`
	Security          Security                `toml:"security" json:"security"`
	TSDB              TSDB                    `toml:"tsdb" json:"tsdb"`
	DocDB             docdb.BadgerConfig      `toml:"docdb" json:"docdb"`
}

var defaultConfig = Config{
	Address: "0.0.0.0:12020",
	PD: PD{
		Endpoints: []string{"127.0.0.1:2379"},
	},
	Log: Log{
		Path:  "", // default output is stdout
		Level: "INFO",
	},
	Storage: Storage{
		Path:         "data",
		SQLiteUseWAL: true,
	},
	ContinueProfiling: ContinueProfilingConfig{
		Enable:               false, // TODO(mornyx): Enable when tiflash#5285 is fixed.
		ProfileSeconds:       10,
		IntervalSeconds:      60,
		TimeoutSeconds:       120,
		DataRetentionSeconds: 3 * 24 * 60 * 60, // 3 days
	},
	TSDB: TSDB{
		RetentionPeriod:           "1", // 1 month
		SearchMaxUniqueTimeseries: 300000,
	},
	DocDB: docdb.BadgerConfig{
		LSMOnly:                 false,
		SyncWrites:              false,
		NumVersionsToKeep:       1,
		NumGoroutines:           8,
		MemTableSize:            64 << 20,
		BaseTableSize:           2 << 20,
		BaseLevelSize:           10 << 20,
		LevelSizeMultiplier:     10,
		MaxLevels:               7,
		VLogPercentile:          0.0,
		ValueThreshold:          1 << 20,
		NumMemtables:            5,
		BlockSize:               4 * 1024,
		BloomFalsePositive:      0.01,
		BlockCacheSize:          256 << 20,
		IndexCacheSize:          0,
		NumLevelZeroTables:      5,
		NumLevelZeroTablesStall: 15,
		ValueLogFileSize:        1<<30 - 1,
		ValueLogMaxEntries:      1000000,
		NumCompactors:           4,
		ZSTDCompressionLevel:    1,
	},
}

func GetDefaultConfig() Config {
	return defaultConfig
}

type Subscriber = chan GetLatestConfig
type GetLatestConfig = func() Config

var (
	globalConfigMutex sync.Mutex
	globalConfig      = defaultConfig

	subscribersMutex        sync.Mutex
	configChangeSubscribers []Subscriber
)

// Subscribe returns a channel that receives a config getter every
// time the config is changed. By calling the getter, you can get
// the latest config.
//
// There will be one getter in the channel after subscribing. It
// can be used to get the current config immediately as follows.
// ```go
// cfgSubscriber := config.Subscribe()
// getCurrentCfg := <-cfgSubscriber
// currentCfg := getCurrentCfg()
// ```
func Subscribe() Subscriber {
	subscribersMutex.Lock()
	defer subscribersMutex.Unlock()

	ch := make(chan GetLatestConfig, 1)
	configChangeSubscribers = append(configChangeSubscribers, ch)
	ch <- GetGlobalConfig
	return ch
}

func notifyConfigChange() {
	subscribersMutex.Lock()
	defer subscribersMutex.Unlock()

	for _, ch := range configChangeSubscribers {
		select {
		case ch <- GetGlobalConfig:
		default:
		}
	}
}

func GetGlobalConfig() (res Config) {
	globalConfigMutex.Lock()
	res = globalConfig
	globalConfigMutex.Unlock()
	return
}

// StoreGlobalConfig stores a new config to the globalConf. It mostly uses in the test to avoid some data races.
func StoreGlobalConfig(config Config) {
	globalConfigMutex.Lock()
	globalConfig = config
	globalConfigMutex.Unlock()
	notifyConfigChange()
}

// UpdateGlobalConfig accesses an update function to update the global config
func UpdateGlobalConfig(update func(Config) Config) {
	globalConfigMutex.Lock()
	globalConfig = update(globalConfig)
	globalConfigMutex.Unlock()
	notifyConfigChange()
}

func InitConfig(configPath string, override func(config *Config)) (*Config, error) {
	config := defaultConfig

	if len(configPath) > 0 {
		if err := config.Load(configPath); err != nil {
			return nil, err
		}
	}

	override(&config)

	config.trimFiledSpace()
	config.setDefaultAdvertiseAddress()

	if err := config.valid(); err != nil {
		return nil, err
	}
	StoreGlobalConfig(config)
	return &config, nil
}

func (c *Config) trimFiledSpace() {
	c.Address = strings.TrimSpace(c.Address)
	c.AdvertiseAddress = strings.TrimSpace(c.AdvertiseAddress)
	for i, addr := range c.PD.Endpoints {
		c.PD.Endpoints[i] = strings.TrimSpace(addr)
	}
}

func (c *Config) Load(fileName string) error {
	_, err := toml.DecodeFile(fileName, c)
	return err
}

func (c *Config) setDefaultAdvertiseAddress() {
	if len(c.AdvertiseAddress) == 0 && strings.HasPrefix(c.Address, "0.0.0.0") {
		ip := utils.GetLocalIP()
		c.AdvertiseAddress = strings.Replace(c.Address, "0.0.0.0", ip, 1)
	}
	if len(c.AdvertiseAddress) == 0 {
		c.AdvertiseAddress = c.Address
	}
}

func (c *Config) valid() error {
	var err error

	if err = validateAddress(c.Address, "address"); err != nil {
		return err
	}

	if err = validateAddress(c.AdvertiseAddress, "advertise-address"); err != nil {
		return err
	}

	if len(c.Address) == 0 {
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

func validateAddress(address, name string) error {
	if len(address) == 0 {
		return fmt.Errorf("unexpected empty %v", name)
	}
	_, port, err := net.SplitHostPort(address)
	if err == nil {
		var p int
		p, err = strconv.Atoi(port)
		if err == nil && p == 0 {
			err = fmt.Errorf("port cannot be set to 0")
		}
	}
	if err != nil {
		return fmt.Errorf("%v %v is invalid, err: %v", name, address, err)
	}
	return nil
}

type Go struct {
	GCPercent   int   `toml:"gc-percent" json:"gc_percent"`
	MemoryLimit int64 `toml:"memory-limit" json:"memory_limit"`
}

type PD struct {
	Endpoints []string `toml:"endpoints" json:"endpoints"`
}

func (p *PD) valid() error {
	if len(p.Endpoints) == 0 {
		return fmt.Errorf("unexpected empty pd endpoints, please specify at least one, e.g. --pd.endpoints \"127.0.0.1:2379\"")
	}

	return nil
}

func (p *PD) Equal(other PD) bool {
	sort.Strings(p.Endpoints)
	sort.Strings(other.Endpoints)

	if len(p.Endpoints) != len(other.Endpoints) {
		return false
	}
	for i := range p.Endpoints {
		if p.Endpoints[i] != other.Endpoints[i] {
			return false
		}
	}
	return true
}

type Storage struct {
	Path              string `toml:"path" json:"path"`
	DocDBBackend      string `toml:"docdb-backend" json:"docdb_backend"`
	SQLiteUseWAL      bool   `toml:"sqlite-use-wal" json:"sqlite_use_wal"`
	MetaRetentionSecs int64  `toml:"meta-retention-secs" json:"meta_retention_secs"`
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

const (
	LevelDebug = "DEBUG"
	LevelInfo  = "INFO"
	LevelWarn  = "WARN"
	LevelError = "ERROR"
)

func (l *Log) valid() error {
	if len(l.Level) == 0 {
		return fmt.Errorf("unexpected empty log level")
	}

	switch l.Level {
	case LevelDebug, LevelInfo, LevelWarn, LevelError:
	default:
		return fmt.Errorf("log level should be %s, %s, %s or %s", LevelDebug, LevelInfo, LevelWarn, LevelError)
	}

	return nil
}

func (l *Log) InitDefaultLogger() {
	cfg := &log.Config{Level: strings.ToLower(l.Level)}
	if l.Path != "" {
		cfg.File = log.FileLogConfig{Filename: path.Join(l.Path, "ng.log")}
	}

	logger, p, err := log.InitLogger(cfg)
	if err != nil {
		stdlog.Fatalf("Failed to init logger, err: %v", err)
	}
	log.ReplaceGlobals(logger, p)
}

func ReloadRoutine(ctx context.Context, configPath string) {
	if len(configPath) == 0 {
		log.Warn("failed to reload config due to empty config path. Please specify the command line argument \"--config <path>\"")
		return
	}
	sighupCh := procutil.NewSighupChan()
	for {
		select {
		case <-ctx.Done():
			return
		case <-sighupCh:
			log.Info("received SIGHUP and ready to reload config")
		}
		newCfg := new(Config)

		if err := newCfg.Load(configPath); err != nil {
			log.Warn("failed to reload config", zap.Error(err))
			continue
		}

		if len(newCfg.PD.Endpoints) == 0 {
			log.Warn("unexpected empty PD endpoints")
			continue
		}

		UpdateGlobalConfig(func(curCfg Config) Config {
			if curCfg.PD.Equal(newCfg.PD) {
				return curCfg
			}

			curCfg.PD = newCfg.PD
			log.Info("PD endpoints changed", zap.Strings("endpoints", curCfg.PD.Endpoints))
			return curCfg
		})
	}
}

func (c *Config) GetHTTPScheme() string {
	if c.Security.GetTLSConfig() != nil {
		return "https"
	}
	return "http"
}

type Security struct {
	SSLCA     string      `toml:"ca-path" json:"ca_path"`
	SSLCert   string      `toml:"cert-path" json:"cert_path"`
	SSLKey    string      `toml:"key-path" json:"key_path"`
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

type TSDB struct {
	RetentionPeriod                  string  `toml:"retention-period" json:"retention_period"`
	SearchMaxUniqueTimeseries        int64   `toml:"search-max-unique-timeseries" json:"search_max_unique_timeseries"`
	MemoryAllowedBytes               int64   `toml:"memory-allowed-bytes" json:"memory_allowed_bytes"`
	MemoryAllowedPercent             float64 `toml:"memory-allowed-percent" json:"memory_allowed_percent"`
	CacheSizeIndexDBDataBlocks       string  `toml:"cache-size-indexdb-data-blocks" json:"cache_size_indexdb_data_blocks"`
	CacheSizeIndexDBDataBlocksSparse string  `toml:"cache-size-indexdb-data-blocks-sparse" json:"cache_size_indexdb_data_blocks_sparse"`
	CacheSizeIndexDBIndexBlocks      string  `toml:"cache-size-indexdb-index-blocks" json:"cache_size_indexdb_index_blocks"`
	CacheSizeIndexDBTagFilters       string  `toml:"cache-size-indexdb-tag-filters" json:"cache_size_indexdb_tag_filters"`
	CacheSizeMetricNamesStats        string  `toml:"cache-size-metric-names-stats" json:"cache_size_metric_names_stats"`
	CacheSizeStorageTSID             string  `toml:"cache-size-storage-tsid" json:"cache_size_storage_tsid"`
}

type ContinueProfilingConfig struct {
	Enable               bool `json:"enable"`
	ProfileSeconds       int  `json:"profile_seconds"`
	IntervalSeconds      int  `json:"interval_seconds"`
	TimeoutSeconds       int  `json:"timeout_seconds"`
	DataRetentionSeconds int  `json:"data_retention_seconds"`
}

func (c ContinueProfilingConfig) Valid() bool {
	if c.ProfileSeconds == 0 ||
		c.IntervalSeconds == 0 ||
		c.TimeoutSeconds == 0 ||
		c.DataRetentionSeconds == 0 {
		return false
	}
	if c.ProfileSeconds > c.IntervalSeconds ||
		c.ProfileSeconds > c.TimeoutSeconds {
		return false
	}
	return true
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
