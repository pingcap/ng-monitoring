// +build !arm !arm64

package timeseries

import (
    "syscall"
)

func Dup2(oldfd int, newfd int) error {
    return syscall.Dup2(oldfd, newfd)
}
