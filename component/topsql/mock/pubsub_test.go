package mock

import (
	"context"
	"testing"
	"time"

	rua "github.com/pingcap/kvproto/pkg/resource_usage_agent"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestStreamAccessorCancellation(t *testing.T) {
	accessors := []struct {
		name string
		call func(context.Context, *MockPubSub) error
	}{
		{"tidb", func(ctx context.Context, s *MockPubSub) error {
			return s.AccessTiDBStream(ctx, func(tipb.TopSQLPubSub_SubscribeServer) error { return nil })
		}},
		{"tikv", func(ctx context.Context, s *MockPubSub) error {
			return s.AccessTiKVStream(ctx, func(rua.ResourceMeteringPubSub_SubscribeServer) error { return nil })
		}},
	}

	for _, accessor := range accessors {
		t.Run(accessor.name, func(t *testing.T) {
			for _, scenario := range []string{"deadline", "caller cancellation", "server stopped"} {
				t.Run(scenario, func(t *testing.T) {
					s := NewMockPubSub()
					defer s.cancel()
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					wantErr := context.Canceled
					if scenario == "deadline" {
						deadlineCtx, deadlineCancel := context.WithTimeout(ctx, 20*time.Millisecond)
						defer deadlineCancel()
						ctx = deadlineCtx
						wantErr = context.DeadlineExceeded
					}
					if scenario == "server stopped" {
						_, _, err := s.Listen("127.0.0.1:0", nil)
						require.NoError(t, err)
						go s.Serve()
						t.Cleanup(s.Stop)
					}

					// No subscriber can receive the callback: cancellation must
					// release the accessor instead of leaving a blocked send.
					done := make(chan error, 1)
					go func() { done <- accessor.call(ctx, s) }()
					switch scenario {
					case "caller cancellation":
						cancel()
					case "server stopped":
						s.Stop()
					}
					select {
					case err := <-done:
						require.ErrorIs(t, err, wantErr)
					case <-time.After(5 * time.Second):
						t.Fatal("stream accessor did not unblock")
					}
				})
			}
		})
	}
}
