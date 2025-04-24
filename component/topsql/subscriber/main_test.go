package subscriber_test

import (
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*loggingT).flushDaemon"),
<<<<<<< HEAD
=======
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime.init.0.func1"),
>>>>>>> f82a378 (*: upgrade vm; expose tsdb parameters for tuning; optimize memory usage (#296))
	}

	goleak.VerifyTestMain(m, opts...)
}
