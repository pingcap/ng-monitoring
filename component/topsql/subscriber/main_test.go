package subscriber_test

import (
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*loggingT).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
	}

	goleak.VerifyTestMain(m, opts...)
}
