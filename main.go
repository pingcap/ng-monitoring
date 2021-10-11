package main

import (
	stdlog "log"
	"os"
	"path"
	"strings"

	"github.com/zhongzc/diag_backend/service"
	"github.com/zhongzc/diag_backend/storage"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/procutil"
	"github.com/pingcap/log"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
)

var (
	listenAddr  = pflag.String("addr", ":8428", "TCP address to listen for http connections")
	pdEndpoints = pflag.StringArray("pd.endpoints", nil, "Addresses of PD instances within the TiDB cluster. Multiple addresses are separated by commas, e.g. [10.0.0.1:2379,10.0.0.2:2379]")
	logPath     = pflag.String("log.path", "log", "Log path of ng monitoring server")
	dataPath    = pflag.String("storage.path", "data", "Storage path of ng monitoring server")
)

func main() {
	// There are dependencies that use `flag`.
	// For isolation and avoiding conflict, we use another command line parser package `pflag`.
	pflag.Parse()

	checkArgs()
	mustCreateDirs()

	logLevel := "INFO"
	initDefaultLogger(path.Join(*logPath, "ng.log"), logLevel)

	logConfig()

	storage.Init(*logPath, logLevel, *dataPath)
	defer storage.Stop()

	service.Init(*logPath, logLevel, *listenAddr)
	defer service.Stop()

	sig := procutil.WaitForSigterm()
	log.Info("received signal", zap.String("sig", sig.String()))
}

func checkArgs() {
	if len(*pdEndpoints) == 0 {
		stdlog.Fatal("Unexpected empty pd endpoints, please specify at least one, e.g. --pd.endpoints \"[127.0.0.1:2379]\"")
	}

	if len(*logPath) == 0 {
		stdlog.Fatal("Unexpected empty log path")
	}

	if len(*dataPath) == 0 {
		stdlog.Fatal("Unexpected empty data path")
	}
}

func mustCreateDirs() {
	if err := os.MkdirAll(*logPath, os.ModePerm); err != nil {
		stdlog.Fatal("failed to init log path", err)
	}

	if err := os.MkdirAll(*dataPath, os.ModePerm); err != nil {
		stdlog.Fatal("failed to init storage path", err)
	}
}

func initDefaultLogger(logPath, logLevel string) {
	l, p, err := log.InitLogger(&log.Config{
		Level: strings.ToLower(logLevel),
		File:  log.FileLogConfig{Filename: logPath},
	})
	if err != nil {
		stdlog.Fatalf("Failed to init logger, err: %v", err)
	}
	log.ReplaceGlobals(l, p)
}

func logConfig() {
	log.Info("config",
		zap.String("addr", *listenAddr),
		zap.Strings("pd.endpoints", *pdEndpoints),
		zap.String("log.path", *logPath),
		zap.String("storage.path", *dataPath),
	)
}
