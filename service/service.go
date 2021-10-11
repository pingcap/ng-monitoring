package service

import (
	"net"
	"path"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

func Init(logPath string, _logLevel string, listenAddr string) {
	l, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatal("failed to listen",
			zap.String("address", listenAddr),
			zap.Error(err),
		)
	}

	go ServeHTTP(path.Join(logPath, "service.log"), l)

	log.Info(
		"starting http service",
		zap.String("address", listenAddr),
	)
}

func Stop() {
	log.Info("shutting down http service")
	StopHTTP()
}
