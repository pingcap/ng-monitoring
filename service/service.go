package service

import (
	"net"
	"net/http"

	"github.com/pingcap/log"
	"github.com/soheilhy/cmux"
	"go.uber.org/zap"
)

var (
	cm cmux.CMux = nil
)

func Init(listenAddr string) {
	l, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatal("failed to listen",
			zap.String("address", listenAddr),
			zap.Error(err),
		)
	}

	cm = cmux.New(l)
	httpL := cm.Match(cmux.HTTP1Fast())
	grpcL := cm.Match(cmux.HTTP2())

	go ServeHTTP(httpL)
	go ServeGRPC(grpcL)
	go func() {
		if err := cm.Serve(); err != nil &&
			err != cmux.ErrServerClosed &&
			err != cmux.ErrListenerClosed &&
			err != http.ErrServerClosed {
			log.Warn("failed to serve http/grpc service",
				zap.String("address", listenAddr),
				zap.Error(err),
			)
		}
	}()

	log.Info(
		"starting http/grpc service",
		zap.String("address", listenAddr),
	)
}

func Stop() {
	if cm == nil {
		return
	}

	log.Info("shutting down http/grpc service")
	cm.Close()
	StopHTTP()
	StopGRPC()
}
