package timeseries

import (
	"syscall"
)

func dup2(oldfd int, newfd int) error {
	return syscall.Dup3(oldfd, newfd, 0)
}
