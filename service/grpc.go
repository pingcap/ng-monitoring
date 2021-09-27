package service

import (
	"io"
	"net"

	"github.com/zhongzc/diag_backend/storage/store"

	"github.com/pingcap/kvproto/pkg/resource_usage_agent"
	"github.com/pingcap/log"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/soheilhy/cmux"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var (
	server *grpc.Server = nil

	sqlMetaSliceP         = SQLMetaSlicePool{}
	planMetaSliceP        = PlanMetaSlicePool{}
	resourceCPUTimeSliceP = ResourceCPUTimeSlicePool{}
)

func ServeGRPC(listener net.Listener) {
	server = grpc.NewServer()
	service := &grpcService{}

	tipb.RegisterTopSQLAgentServer(server, service)
	resource_usage_agent.RegisterResourceUsageAgentServer(server, service)

	if err := server.Serve(listener); err != nil &&
		err != cmux.ErrListenerClosed &&
		err != cmux.ErrServerClosed &&
		err != grpc.ErrServerStopped {
		log.Warn("failed to serve grpc", zap.Error(err))
	}
}

func StopGRPC() {
	if server == nil {
		return
	}

	log.Info("shutting down grpc server")
	server.GracefulStop()
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
