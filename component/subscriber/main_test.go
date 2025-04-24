package subscriber_test

import (
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime.init.0.func1"),
	}

	goleak.VerifyTestMain(m, opts...)
}
