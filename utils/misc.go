package utils

import (
	"net"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// GoWithRecovery wraps goroutine startup call with force recovery.
// it will dump current goroutine stack into log if catch any recover result.
//
//	exec:      execute logic function.
//	recoverFn: handler will be called after recover and before dump stack, passing `nil` means noop.
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

// GetLocalIP will return a local IP(non-loopback, non 0.0.0.0), if there is one
func GetLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err == nil {
		for _, address := range addrs {
			ipnet, ok := address.(*net.IPNet)
			if ok && ipnet.IP.IsGlobalUnicast() {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}
