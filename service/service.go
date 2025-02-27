package service

import (
	"net"

	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/ng-monitoring/database/docdb"
	"github.com/pingcap/ng-monitoring/service/http"
	"github.com/pingcap/ng-monitoring/utils"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

func Init(cfg *config.Config, docDB docdb.DocDB) {
	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		log.Fatal("failed to listen",
			zap.String("address", cfg.Address),
			zap.Error(err),
		)
	}

	go utils.GoWithRecovery(func() {
		http.ServeHTTP(&cfg.Log, listener, docDB)
	}, nil)

	log.Info(
		"starting http service",
		zap.String("address", cfg.Address),
	)
}

func Stop() {
	http.StopHTTP()
}
