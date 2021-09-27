module github.com/zhongzc/diag_backend

go 1.16

require (
	github.com/VictoriaMetrics/VictoriaMetrics v1.65.0
	github.com/dgraph-io/badger/v3 v3.2103.1
	github.com/genjidb/genji v0.13.0
	github.com/genjidb/genji/engine/badgerengine v0.13.0
	github.com/gin-contrib/cors v1.3.1
	github.com/gin-contrib/gzip v0.0.3
	github.com/gin-gonic/gin v1.7.4
	github.com/go-playground/validator/v10 v10.9.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/mattn/go-isatty v0.0.14 // indirect
	github.com/pingcap/kvproto v0.0.0-20210915062418-0f5764a128ad
	github.com/pingcap/log v0.0.0-20210906054005-afc726e70354
	github.com/pingcap/tipb v0.0.0-20210917081614-311f2369c5f7
	github.com/soheilhy/cmux v0.1.5
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/ugorji/go v1.2.6 // indirect
	github.com/wangjohn/quickselect v0.0.0-20161129230411-ed8402a42d5f
	go.uber.org/zap v1.19.0
	golang.org/x/crypto v0.0.0-20210915214749-c084706c2272 // indirect
	golang.org/x/net v0.0.0-20210924151903-3ad01bbaa167 // indirect
	golang.org/x/sys v0.0.0-20210915083310-ed5796bab164 // indirect
	google.golang.org/grpc v1.40.0
)

replace (
	github.com/pingcap/kvproto => github.com/zhongzc/kvproto v0.0.0-20210923091320-34901926eaa1
	github.com/pingcap/tipb => github.com/zhongzc/tipb v0.0.0-20210923091456-2d029dc0ea80
)
