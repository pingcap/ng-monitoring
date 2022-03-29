//go:build !linux
// +build !linux

package timeseries

import (
	"syscall"
)

func dup2(oldfd int, newfd int) error {
	return syscall.Dup2(oldfd, newfd)
}
