package service

import (
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"io"
	"net"

	"github.com/zhongzc/diag_backend/storage/store"

	"github.com/pingcap/kvproto/pkg/resource_usage_agent"
	"github.com/pingcap/log"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	sqlMetaSliceP         = SQLMetaSlicePool{}
	planMetaSliceP        = PlanMetaSlicePool{}
	resourceCPUTimeSliceP = ResourceCPUTimeSlicePool{}
)

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
		*records = append(*records, req)
	}

	if err := store.TopSQLRecords(*records); err != nil {
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
		*records = append(*records, req)
	}

	if err := store.SQLMetas(*records); err != nil {
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
		*records = append(*records, req)
	}

	if err := store.PlanMetas(*records); err != nil {
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
		*records = append(*records, req)
	}

	if err := store.ResourceMeteringRecords(*records); err != nil {
		log.Warn("failed to store TopSQL resource metering records", zap.Error(err))
	}

	return stream.SendAndClose(&resource_usage_agent.EmptyResponse{})
}
