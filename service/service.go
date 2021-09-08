package service

import (
    "flag"
    "fmt"
    "net/http"

    "github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert"
    "github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect"
    "github.com/VictoriaMetrics/VictoriaMetrics/app/vmstorage"
    "github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
    "github.com/pingcap/log"
    "github.com/zhongzc/diag_backend/service/kv"
    "go.uber.org/zap"
)

var httpListenAddr = flag.String("httpListenAddr", ":8428", "TCP address to listen for http connections")

func Init() {
    go httpserver.Serve(*httpListenAddr, requestHandler)
    log.Info("starting webservice", zap.String("address", *httpListenAddr))
}

func Stop() {
    log.Info("gracefully shutting down webservice", zap.String("address", *httpListenAddr))
    if err := httpserver.Stop(*httpListenAddr); err != nil {
        log.Info("cannot stop the webservice", zap.Error(err))
    }
}

func requestHandler(w http.ResponseWriter, r *http.Request) bool {
    if r.URL.Path == "/" {
        if r.Method != "GET" {
            return false
        }
        fmt.Fprintf(w, "<h2>Single-node VictoriaMetrics</h2></br>")
        fmt.Fprintf(w, "See docs at <a href='https://docs.victoriametrics.com/'>https://docs.victoriametrics.com/</a></br>")
        fmt.Fprintf(w, "Useful endpoints:</br>")
        httpserver.WriteAPIHelp(w, [][2]string{
            {"/vmui", "Web UI"},
            {"/targets", "discovered targets list"},
            {"/api/v1/targets", "advanced information about discovered targets in JSON format"},
            {"/metrics", "available service metrics"},
            {"/api/v1/status/tsdb", "tsdb status page"},
            {"/api/v1/status/top_queries", "top queries"},
            {"/api/v1/status/active_queries", "active queries"},
        })
        return true
    }
    if vminsert.RequestHandler(w, r) {
        return true
    }
    if vmselect.RequestHandler(w, r) {
        return true
    }
    if vmstorage.RequestHandler(w, r) {
        return true
    }
    if kv.RequestHandler(w, r) {
        return true
    }
    return false
}
