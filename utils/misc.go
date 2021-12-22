package utils

import (
	"context"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// GoWithRecovery wraps goroutine startup call with force recovery.
// it will dump current goroutine stack into log if catch any recover result.
//   exec:      execute logic function.
//   recoverFn: handler will be called after recover and before dump stack, passing `nil` means noop.
func GoWithRecovery(exec func(), recoverFn func(r interface{})) {
	defer func() {
		r := recover()
		if recoverFn != nil {
			recoverFn(r)
		}
		if r != nil {
			log.Error("panic in the recoverable goroutine",
				zap.Reflect("r", r),
				zap.Stack("stack trace"))
		}
	}()
	exec()
}

// WithRetry provides a general retry logic.
//
// The given f will keep running, until:
// - f returns true. It means work is done.
// - retry times is greater than the given times
// - ctx is done.
//
// Otherwise, this function will wait a time of the given duration
// and continue to execute f.
//
// The argument provided for f is the retried times.
func WithRetry(ctx context.Context, maxTimes uint, duration time.Duration, f func(uint) bool) {
	for retry := uint(0); retry < maxTimes; retry++ {
		if done := f(retry + 1); done {
			return
		}
		if retry < maxTimes {
			select {
			case <-time.After(duration):
			case <-ctx.Done():
				return
			}
		}
	}
}

// WithRetryBackoff provides a general retry logic.
//
// The given f will keep running, until:
// - f returns true. It means work is done.
// - retry times is greater than the given times
// - ctx is done.
//
// Otherwise, this function will wait a time of the given duration
// in a backoff way, and continue to execute f.
//
// The argument provided for f is the retried times.
func WithRetryBackoff(ctx context.Context, maxTimes uint, firstDuration time.Duration, f func(uint) bool) {
	duration := firstDuration
	for retry := uint(0); retry < maxTimes; retry++ {
		if done := f(retry + 1); done {
			return
		}
		if retry < maxTimes {
			select {
			case <-time.After(duration):
			case <-ctx.Done():
				return
			}
			duration *= 2
		}
	}
}
