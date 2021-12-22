package utils

import (
	"context"
	"time"
)

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
func WithRetry(ctx context.Context, maxRetryTimes uint, duration time.Duration, f func(uint) bool) {
	for retried := uint(0); retried <= maxRetryTimes; retried++ {
		if done := f(retried); done {
			return
		}
		if retried < maxRetryTimes {
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
func WithRetryBackoff(ctx context.Context, maxRetryTimes uint, firstDuration time.Duration, f func(uint) bool) {
	duration := firstDuration
	for retried := uint(0); retried <= maxRetryTimes; retried++ {
		if done := f(retried); done {
			return
		}
		if retried < maxRetryTimes {
			select {
			case <-time.After(duration):
			case <-ctx.Done():
				return
			}
			duration *= 2
		}
	}
}
