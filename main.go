package main

import (
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/procutil"
	"github.com/pingcap/log"
	"github.com/zhongzc/diag_backend/kv"
	"github.com/zhongzc/diag_backend/service"
	"github.com/zhongzc/diag_backend/vm"
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
