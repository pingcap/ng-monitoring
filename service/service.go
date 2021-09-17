package service

import (
    "io"
    "net"
    "net/http"
    "strconv"
    "time"

    "github.com/zhongzc/diag_backend/storage/query"
    "github.com/zhongzc/diag_backend/storage/store"

    "github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert"
    "github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect"
    "github.com/VictoriaMetrics/VictoriaMetrics/app/vmstorage"
    "github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
    "github.com/gin-gonic/gin"
    "github.com/pingcap/kvproto/pkg/resource_usage_agent"
    "github.com/pingcap/log"
    "github.com/pingcap/tipb/go-tipb"
    "go.uber.org/zap"
    "google.golang.org/grpc"
)

var (
    httpListenAddr = ""
    grpcListenAddr = ""

    ginEngine = gin.Default()

    cpuTimeSliceP         = CPUTimeSlicePool{}
    sqlMetaSliceP         = SQLMetaSlicePool{}
    planMetaSliceP        = PlanMetaSlicePool{}
    resourceCPUTimeSliceP = ResourceCPUTimeSlicePool{}
    topSQLItemsP          = TopSQLItemsPool{}
    instanceItemsP        = InstanceItemsPool{}
)

func Init(httpAddr, grpcAddr string) {
    httpListenAddr = httpAddr
    grpcListenAddr = grpcAddr

    initGinHandler()

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

func requestHandler(w http.ResponseWriter, r *http.Request) bool {
    if vminsert.RequestHandler(w, r) {
        return true
    }
    if vmselect.RequestHandler(w, r) {
        return true
    }
    if vmstorage.RequestHandler(w, r) {
        return true
    }

    ginEngine.ServeHTTP(w, r)
    return true
}

func ServeGRPC(addr string) {
    server := grpc.NewServer()
    service := &grpcService{}

    tipb.RegisterTopSQLAgentServer(server, service)
    resource_usage_agent.RegisterResourceUsageAgentServer(server, service)

    listener, err := net.Listen("tcp", addr)
    if err != nil {
        log.Fatal("failed to listen", zap.String("addr", addr), zap.Error(err))
    }
    err = server.Serve(listener)
    if err != nil {
        log.Fatal("failed to serve grpc", zap.String("addr", addr), zap.Error(err))
    }
}

func initGinHandler() {
    ginEngine.GET("/topsql/v1/cpu_time", topSQLCPUTime)
    ginEngine.GET("/topsql/v1/instances", topSQLAllInstances)
}

func topSQLCPUTime(c *gin.Context) {
    instance := c.Query("instance")
    if len(instance) == 0 {
        c.JSON(http.StatusBadRequest, gin.H{
            "status":  "error",
            "message": "no instance",
        })
        return
    }

    var err error
    now := time.Now().Unix()

    var startSecs int64
    var endSecs int64
    var top int64
    var windowSecs int64

    raw := c.DefaultQuery("startSecs", strconv.Itoa(int(now-6*60*60)))
    startSecs, err = strconv.ParseInt(raw, 10, 64)
    if err != nil {
        c.JSON(http.StatusBadRequest, gin.H{
            "status":  "error",
            "message": err.Error(),
        })
        return
    }

    raw = c.DefaultQuery("endSecs", strconv.Itoa(int(now)))
    endSecs, err = strconv.ParseInt(raw, 10, 64)
    if err != nil {
        c.JSON(http.StatusBadRequest, gin.H{
            "status":  "error",
            "message": err.Error(),
        })
        return
    }

    raw = c.DefaultQuery("top", "-1")
    top, err = strconv.ParseInt(raw, 10, 64)
    if err != nil {
        c.JSON(http.StatusBadRequest, gin.H{
            "status":  "error",
            "message": err.Error(),
        })
        return
    }

    raw = c.DefaultQuery("windowSecs", "1m")
    duration, err := time.ParseDuration(raw)
    if err != nil {
        c.JSON(http.StatusBadRequest, gin.H{
            "status":  "error",
            "message": err.Error(),
        })
        return
    }
    windowSecs = int64(duration.Seconds())

    items := topSQLItemsP.Get()
    defer topSQLItemsP.Put(items)

    err = query.TopSQL(startSecs, endSecs, windowSecs, top, instance, &items)
    if err != nil {
        c.JSON(http.StatusServiceUnavailable, gin.H{
            "status":  "error",
            "message": err.Error(),
        })
        return
    }

    c.JSON(http.StatusOK, gin.H{
        "status": "ok",
        "data":   items,
    })
}

func topSQLAllInstances(c *gin.Context) {
    instances := instanceItemsP.Get()
    defer instanceItemsP.Put(instances)

    if err := query.AllInstances(&instances); err != nil {
        c.JSON(http.StatusServiceUnavailable, gin.H{
            "status":  "error",
            "message": err.Error(),
        })
        return
    }

    c.JSON(http.StatusOK, gin.H{
        "status": "ok",
        "data":   instances,
    })
}

type grpcService struct{}

var _ tipb.TopSQLAgentServer = &grpcService{}
var _ resource_usage_agent.ResourceUsageAgentServer = &grpcService{}

func (g *grpcService) ReportCPUTimeRecords(stream tipb.TopSQLAgent_ReportCPUTimeRecordsServer) error {
    records := cpuTimeSliceP.Get()
    defer cpuTimeSliceP.Put(records)

    for {
        req, err := stream.Recv()
        if err == io.EOF {
            break
        } else if err != nil {
            return err
        }
        records = append(records, req)
    }

    if err := store.TopSQLRecords(records); err != nil {
        log.Warn("failed to storage top sql records", zap.Error(err))
    }

    return stream.SendAndClose(&tipb.EmptyResponse{})
}

func (g *grpcService) ReportSQLMeta(stream tipb.TopSQLAgent_ReportSQLMetaServer) error {
    records := sqlMetaSliceP.Get()
    defer sqlMetaSliceP.Put(records)

    for {
        req, err := stream.Recv()
        if err == io.EOF {
            break
        } else if err != nil {
            return err
        }
        records = append(records, req)
    }

    if err := store.SQLMetas(records); err != nil {
        log.Warn("failed to storage TopSQL SQL metas", zap.Error(err))
    }

    return stream.SendAndClose(&tipb.EmptyResponse{})
}

func (g *grpcService) ReportPlanMeta(stream tipb.TopSQLAgent_ReportPlanMetaServer) error {
    records := planMetaSliceP.Get()
    defer planMetaSliceP.Put(records)

    for {
        req, err := stream.Recv()
        if err == io.EOF {
            break
        } else if err != nil {
            return err
        }
        records = append(records, req)
    }

    if err := store.PlanMetas(records); err != nil {
        log.Warn("failed to storage TopSQL plan metas", zap.Error(err))
    }

    return stream.SendAndClose(&tipb.EmptyResponse{})
}

func (g *grpcService) ReportCPUTime(stream resource_usage_agent.ResourceUsageAgent_ReportCPUTimeServer) error {
    records := resourceCPUTimeSliceP.Get()
    defer resourceCPUTimeSliceP.Put(records)

    for {
        req, err := stream.Recv()
        if err == io.EOF {
            break
        } else if err != nil {
            return err
        }
        records = append(records, req)
    }

    if err := store.ResourceMeteringRecords(records); err != nil {
        log.Warn("failed to storage TopSQL resource metering records", zap.Error(err))
    }

    return stream.SendAndClose(&resource_usage_agent.EmptyResponse{})
}
