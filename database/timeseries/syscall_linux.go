package timeseries

import (
	"syscall"
)

func dup(fd int) (int, error) {
	return syscall.Dup(fd)
}

func dup2(oldfd int, newfd int) error {
	return syscall.Dup3(oldfd, newfd, 0)
}
