package main

import (
    "flag"

    "github.com/zhongzc/diag_backend/service"
    "github.com/zhongzc/diag_backend/storage"

    "github.com/VictoriaMetrics/VictoriaMetrics/lib/procutil"
    "github.com/pingcap/log"
    "go.uber.org/zap"
)

var (
    httpListenAddr = flag.String("httpListenAddr", ":8428", "TCP address to listen for http connections")
    grpcListenAddr = flag.String("grpcListenAddr", ":8484", "TCP address to listen for grpc connections")
)

func main() {
    flag.Parse()

    storage.Init(*httpListenAddr)
    defer storage.Stop()

    service.Init(*httpListenAddr, *grpcListenAddr)
    defer service.Stop()

    sig := procutil.WaitForSigterm()
    log.Info("received signal", zap.String("sig", sig.String()))
}
