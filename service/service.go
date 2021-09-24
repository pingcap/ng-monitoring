package service

import (
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

var (
	httpListenAddr = ""
	grpcListenAddr = ""
)

func Init(httpAddr, grpcAddr string) {
	httpListenAddr = httpAddr
	grpcListenAddr = grpcAddr

	go httpserver.Serve(httpListenAddr, requestHandler)
	go ServeGRPC(grpcListenAddr)

	log.Info(
		"starting webservice",
		zap.String("http-address", httpListenAddr),
		zap.String("grpc-address", grpcListenAddr),
	)
}

func Stop() {
	log.Info("gracefully shutting down webservice", zap.String("address", httpListenAddr))
	if err := httpserver.Stop(httpListenAddr); err != nil {
		log.Info("cannot stop the webservice", zap.Error(err))
	}
}
