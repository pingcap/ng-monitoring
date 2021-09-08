package main

import (
    "github.com/VictoriaMetrics/VictoriaMetrics/lib/procutil"
    "github.com/pingcap/log"
    "github.com/zhongzc/tmp/kv"
    "github.com/zhongzc/tmp/service"
    "github.com/zhongzc/tmp/vm"
    "go.uber.org/zap"
)

func main() {
    vm.Init()
    defer vm.Stop()

    kv.Init()
    defer kv.Stop()

    service.Init()
    defer service.Stop()

    sig := procutil.WaitForSigterm()
    log.Info("received signal", zap.String("sig", sig.String()))
}
