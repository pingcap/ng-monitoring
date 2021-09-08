package vm

import (
    "flag"
    "os"
    "time"

    "github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert"
    "github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect"
    "github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/promql"
    "github.com/VictoriaMetrics/VictoriaMetrics/app/vmstorage"
    "github.com/VictoriaMetrics/VictoriaMetrics/lib/buildinfo"
    "github.com/VictoriaMetrics/VictoriaMetrics/lib/envflag"
    "github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
    "github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
    "github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
    "github.com/VictoriaMetrics/VictoriaMetrics/lib/promscrape"
    "github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

var (
    minScrapeInterval = flag.Duration("dedup.minScrapeInterval", 0, "Leave only the first sample in every time series per each discrete interval "+
        "equal to -dedup.minScrapeInterval > 0. See https://docs.victoriametrics.com/#deduplication for details")
    dryRun = flag.Bool("dryRun", false, "Whether to check only -promscrape.config and then exit. "+
        "Unknown config entries are allowed in -promscrape.config by default. This can be changed with -promscrape.config.strictParse")
)

func Init() {
    // Write flags and help message to stdout, since it is easier to grep or pipe.
    flag.CommandLine.SetOutput(os.Stdout)
    flag.Usage = usage
    envflag.Parse()
    buildinfo.Init()
    logger.Init()

    if promscrape.IsDryRun() {
        *dryRun = true
    }
    if *dryRun {
        if err := promscrape.CheckConfig(); err != nil {
            logger.Fatalf("error when checking -promscrape.config: %s", err)
        }
        logger.Infof("-promscrape.config is ok; exitting with 0 status code")
        return
    }

    startTime := time.Now()
    storage.SetMinScrapeIntervalForDeduplication(*minScrapeInterval)
    vmstorage.Init(promql.ResetRollupResultCacheIfNeeded)
    vmselect.Init()
    vminsert.Init()
    startSelfScraper()

    logger.Infof("started VictoriaMetrics in %.3f seconds", time.Since(startTime).Seconds())
}

func Stop() {
    stopSelfScraper()

    startTime := time.Now()
    vminsert.Stop()
    logger.Infof("successfully shut down the webservice in %.3f seconds", time.Since(startTime).Seconds())

    vmstorage.Stop()
    vmselect.Stop()

    fs.MustStopDirRemover()

    logger.Infof("the VictoriaMetrics has been stopped in %.3f seconds", time.Since(startTime).Seconds())
}

func usage() {
    const s = `
victoria-metrics is a time series database and monitoring solution.

See the docs at https://docs.victoriametrics.com/
`
    flagutil.Usage(s)
}
