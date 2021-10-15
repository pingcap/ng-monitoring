package main

import (
	"context"

	stdlog "log"
	"os"

	"github.com/zhongzc/ng_monitoring/component/continuousprofiling"
	"github.com/zhongzc/ng_monitoring/component/topsql"
	"github.com/zhongzc/ng_monitoring/config"
	"github.com/zhongzc/ng_monitoring/database"
	"github.com/zhongzc/ng_monitoring/database/document"
	"github.com/zhongzc/ng_monitoring/database/timeseries"
	"github.com/zhongzc/ng_monitoring/service"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/procutil"
	"github.com/pingcap/log"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
)

const (
	nmAddr        = "addr"
	nmPdEndpoints = "pd.endpoints"
	nmLogPath     = "log.path"
	nmStoragePath = "storage.path"
	nmConfig      = "config"
)

var (
	listenAddr  = pflag.String(nmAddr, "", "TCP address to listen for http connections")
	pdEndpoints = pflag.StringArray(nmPdEndpoints, nil, "Addresses of PD instances within the TiDB cluster. Multiple addresses are separated by commas, e.g. [10.0.0.1:2379,10.0.0.2:2379]")
	logPath     = pflag.String(nmLogPath, "", "Log path of ng monitoring server")
	storagePath = pflag.String(nmStoragePath, "", "Storage path of ng monitoring server")
	configPath  = pflag.String(nmConfig, "", "config file path")
)

func main() {
	// There are dependencies that use `flag`.
	// For isolation and avoiding conflict, we use another command line parser package `pflag`.
	pflag.Parse()

	cfg, err := config.InitConfig(*configPath, overrideConfig)
	if err != nil {
		stdlog.Fatalf("Failed to initialize config, err: %s", err.Error())
	}

	cfg.Log.InitDefaultLogger()
	log.Info("config", zap.Any("config", cfg))

	mustCreateDirs(cfg)

	database.Init(cfg)
	defer database.Stop()

	topsql.Init(document.Get(), timeseries.InsertHandler, timeseries.SelectHandler)
	defer topsql.Stop()

	err = continuousprofiling.Init(document.Get(), cfg)
	if err != nil {
		stdlog.Fatalf("Failed to initialize continuous profiling, err: %s", err.Error())
	}
	defer continuousprofiling.Stop()

	service.Init(cfg)
	defer service.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go config.ReloadRoutine(ctx, *configPath, cfg)
	sig := procutil.WaitForSigterm()
	log.Info("received signal", zap.String("sig", sig.String()))
}

func overrideConfig(config *config.Config) {
	pflag.Visit(func(f *pflag.Flag) {
		switch f.Name {
		case nmAddr:
			config.Addr = *listenAddr
		case nmPdEndpoints:
			config.PD.Endpoints = *pdEndpoints
		case nmLogPath:
			config.Log.Path = *logPath
		case nmStoragePath:
			config.Storage.Path = *storagePath
		}
	})
}

func mustCreateDirs(config *config.Config) {
	if err := os.MkdirAll(config.Log.Path, os.ModePerm); err != nil {
		log.Fatal("failed to init log path", zap.Error(err))
	}

	if err := os.MkdirAll(config.Storage.Path, os.ModePerm); err != nil {
		log.Fatal("failed to init storage path", zap.Error(err))
	}
}
