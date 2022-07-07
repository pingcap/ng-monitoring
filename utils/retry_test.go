package utils_test

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/ng-monitoring/utils"

	"github.com/stretchr/testify/require"
)

func TestWithRetry(t *testing.T) {
	t.Parallel()

	maxRetryTimes := uint(10)
	executed := uint(0)
	utils.WithRetry(context.Background(), maxRetryTimes, 1*time.Millisecond, func(u uint) bool {
		require.Equal(t, u, executed)
		executed += 1
		return false
	})
	require.Equal(t, maxRetryTimes+1, executed)
}

func TestWithRetryCtxDone(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	maxRetryTimes := uint(10)
	executed := uint(0)
	utils.WithRetry(ctx, maxRetryTimes, 1*time.Minute, func(u uint) bool {
		require.Equal(t, u, executed)
		executed += 1
		return false
	})

	require.Equal(t, executed, uint(1))
}

func TestWithRetryDoneMidway(t *testing.T) {
	t.Parallel()

	maxRetryTimes := uint(10)
	executed := uint(0)
	utils.WithRetry(context.Background(), maxRetryTimes, 1*time.Millisecond, func(u uint) bool {
		require.Equal(t, u, executed)
		executed += 1

		return u == 3
	})
	require.Equal(t, executed, uint(4))
}

func TestWithRetryBackoff(t *testing.T) {
	t.Parallel()

	now := time.Now()
	maxRetryTimes := uint(10)
	executed := uint(0)
	utils.WithRetryBackoff(context.Background(), maxRetryTimes, 1*time.Millisecond, func(u uint) bool {
		require.Equal(t, u, executed)
		executed += 1
		return false
	})
	require.Equal(t, maxRetryTimes+1, executed)

	// wait about 1023 millis
	require.Greater(t, time.Since(now), 1*time.Second)
}

func TestWithRetryBackoffCtxDone(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	maxRetryTimes := uint(10)
	executed := uint(0)
	utils.WithRetryBackoff(ctx, maxRetryTimes, 1*time.Minute, func(u uint) bool {
		require.Equal(t, u, executed)
		executed += 1
		return false
	})

	require.Equal(t, executed, uint(1))
}

func TestWithRetryBackoffDoneMidway(t *testing.T) {
	t.Parallel()

	maxRetryTimes := uint(10)
	executed := uint(0)
	utils.WithRetryBackoff(context.Background(), maxRetryTimes, 1*time.Millisecond, func(u uint) bool {
		require.Equal(t, u, executed)
		executed += 1

		return u == 3
	})
	require.Equal(t, executed, uint(4))
}
