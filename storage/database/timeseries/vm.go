package timeseries

import (
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/promql"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

func Init(_ string) {
	logger.Init()

	startTime := time.Now()
	storage.SetMinScrapeIntervalForDeduplication(0)
	vmstorage.Init(promql.ResetRollupResultCacheIfNeeded)
	vmselect.Init()
	vminsert.Init()

	logger.Infof("started VictoriaMetrics in %.3f seconds", time.Since(startTime).Seconds())
}

func Stop() {
	startTime := time.Now()
	vminsert.Stop()
	logger.Infof("successfully shut down the webservice in %.3f seconds", time.Since(startTime).Seconds())

	vmstorage.Stop()
	vmselect.Stop()

	fs.MustStopDirRemover()

	logger.Infof("the VictoriaMetrics has been stopped in %.3f seconds", time.Since(startTime).Seconds())
}
