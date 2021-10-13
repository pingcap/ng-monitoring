package service

import (
	"net"

	"github.com/zhongzc/ng_monitoring/config"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

func Init(cfg *config.Config) {
	listener, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		log.Fatal("failed to listen",
			zap.String("address", cfg.Addr),
			zap.Error(err),
		)
	}

	go ServeHTTP(&cfg.Log, listener)

	log.Info(
		"starting http service",
		zap.String("address", cfg.Addr),
	)
}

func Stop() {
	StopHTTP()
}
