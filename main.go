package main

import (
	"context"
	"fmt"
	stdlog "log"
	"os"

	"github.com/pingcap/ng-monitoring/component/conprof"
	"github.com/pingcap/ng-monitoring/component/domain"
	"github.com/pingcap/ng-monitoring/component/topology"
	"github.com/pingcap/ng-monitoring/component/topsql"
	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/ng-monitoring/config/pdvariable"
	"github.com/pingcap/ng-monitoring/database"
	"github.com/pingcap/ng-monitoring/database/document"
	"github.com/pingcap/ng-monitoring/database/timeseries"
	"github.com/pingcap/ng-monitoring/service"
	"github.com/pingcap/ng-monitoring/utils/printer"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/procutil"
	"github.com/pingcap/log"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
)

const (
	nmVersion          = "version"
	nmAddr             = "address"
	nmPdEndpoints      = "pd.endpoints"
	nmLogPath          = "log.path"
	nmStoragePath      = "storage.path"
	nmConfig           = "config"
	nmAdvertiseAddress = "advertise-address"
	nmRetentionPeriod  = "retention-period"
)

var (
	version          = pflag.BoolP(nmVersion, "V", false, "print version information and exit")
	listenAddr       = pflag.String(nmAddr, "", "TCP address to listen for http connections")
	pdEndpoints      = pflag.StringSlice(nmPdEndpoints, nil, "Addresses of PD instances within the TiDB cluster. Multiple addresses are separated by commas, e.g. --pd.endpoints 10.0.0.1:2379,10.0.0.2:2379")
	logPath          = pflag.String(nmLogPath, "", "Log path of ng monitoring server")
	storagePath      = pflag.String(nmStoragePath, "", "Storage path of ng monitoring server")
	configPath       = pflag.String(nmConfig, "", "config file path")
	advertiseAddress = pflag.String(nmAdvertiseAddress, "", "ngm server advertise IP:PORT")
	retentionPeriod  = pflag.String(nmRetentionPeriod, "", "Data with timestamps outside the retentionPeriod is automatically deleted\nThe following optional suffixes are supported: h (hour), d (day), w (week), y (year). If suffix isn't set, then the duration is counted in months")
)

func main() {
	// There are dependencies that use `flag`.
	// For isolation and avoiding conflict, we use another command line parser package `pflag`.
	pflag.Parse()

	if *version {
		fmt.Println(printer.GetNGMInfo())
		return
	}

	cfg, err := config.InitConfig(*configPath, overrideConfig)
	if err != nil {
		stdlog.Fatalf("Failed to initialize config, err: %s", err.Error())
	}

	cfg.Log.InitDefaultLogger()
	printer.PrintNGMInfo()
	log.Info("config", zap.Any("config", cfg))

	mustCreateDirs(cfg)

	database.Init(cfg)
	defer database.Stop()

	err = config.LoadConfigFromStorage(document.Get)
	if err != nil {
		stdlog.Fatalf("Failed to load config from storage, err: %s", err.Error())
	}

	do := domain.NewDomain()
	defer do.Close()

	err = topology.Init(do)
	if err != nil {
		log.Fatal("Failed to initialize topology", zap.Error(err))
	}
	defer topology.Stop()

	pdvariable.Init(do)
	defer pdvariable.Stop()

	err = topsql.Init(do, config.Subscribe(), document.Get(), timeseries.InsertHandler, timeseries.SelectHandler, topology.Subscribe(), pdvariable.Subscribe())
	if err != nil {
		log.Fatal("Failed to initialize topsql", zap.Error(err))
	}
	defer topsql.Stop()

	err = conprof.Init(document.Get(), topology.Subscribe())
	if err != nil {
		log.Fatal("Failed to initialize continuous profiling", zap.Error(err))
	}
	defer conprof.Stop()

	service.Init(cfg)
	defer service.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go config.ReloadRoutine(ctx, *configPath)
	sig := procutil.WaitForSigterm()
	log.Info("received signal", zap.String("sig", sig.String()))
}

func overrideConfig(config *config.Config) {
	pflag.Visit(func(f *pflag.Flag) {
		switch f.Name {
		case nmAddr:
			config.Address = *listenAddr
		case nmPdEndpoints:
			config.PD.Endpoints = *pdEndpoints
		case nmLogPath:
			config.Log.Path = *logPath
		case nmStoragePath:
			config.Storage.Path = *storagePath
		case nmAdvertiseAddress:
			config.AdvertiseAddress = *advertiseAddress
		case nmRetentionPeriod:
			config.TSDB.RetentionPeriod = *retentionPeriod
		}
	})
}

func mustCreateDirs(config *config.Config) {
	if config.Log.Path != "" {
		if err := os.MkdirAll(config.Log.Path, os.ModePerm); err != nil {
			log.Fatal("failed to init log path", zap.Error(err))
		}
	}

	if err := os.MkdirAll(config.Storage.Path, os.ModePerm); err != nil {
		log.Fatal("failed to init storage path", zap.Error(err))
	}
}
