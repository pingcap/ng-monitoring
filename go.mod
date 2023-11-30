module github.com/pingcap/ng-monitoring

go 1.20

require (
	github.com/BurntSushi/toml v1.2.1
	github.com/VictoriaMetrics/VictoriaMetrics v1.76.1
	github.com/dgraph-io/badger/v3 v3.2103.1
	github.com/genjidb/genji v0.13.0
	github.com/genjidb/genji/engine/badgerengine v0.13.0
	github.com/gin-contrib/pprof v1.3.0
	github.com/gin-gonic/gin v1.8.1
	github.com/goccy/go-graphviz v0.0.9
	github.com/golang/mock v1.6.0
	github.com/golang/protobuf v1.5.2
	github.com/golang/snappy v0.0.4
	github.com/google/pprof v0.0.0-20211122183932-1daafda22083
	github.com/pingcap/kvproto v0.0.0-20230201112839-2b853bed8125
	github.com/pingcap/log v1.1.1-0.20221116035753-734d527bc87c
	github.com/pingcap/tidb v1.1.0-beta.0.20230228104109-c78e413b4ed0
	github.com/pingcap/tidb-dashboard v0.0.0-20230222090907-5530ca41768d
	github.com/pingcap/tipb v0.0.0-20230119054146-c6b7a5a1623b
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.14.0
	github.com/prometheus/common v0.39.0
	github.com/spf13/pflag v1.0.5
<<<<<<< HEAD
	github.com/stretchr/testify v1.8.1
	github.com/valyala/gozstd v1.14.2
=======
	github.com/stretchr/testify v1.8.3
	github.com/valyala/gozstd v1.16.0
>>>>>>> f223e14 (tsdb: update vm to v1.76.1; expose more params (#213))
	github.com/wangjohn/quickselect v0.0.0-20161129230411-ed8402a42d5f
	go.etcd.io/etcd/api/v3 v3.5.7
	go.etcd.io/etcd/client/pkg/v3 v3.5.7
	go.etcd.io/etcd/client/v3 v3.5.7
	go.etcd.io/etcd/tests/v3 v3.5.7
	go.uber.org/atomic v1.10.0
	go.uber.org/goleak v1.2.0
	go.uber.org/zap v1.24.0
	golang.org/x/net v0.7.0
	google.golang.org/grpc v1.51.0
)

require (
	cloud.google.com/go/compute v1.13.0 // indirect
	cloud.google.com/go/compute/metadata v0.2.1 // indirect
	github.com/ReneKroon/ttlcache/v2 v2.3.0 // indirect
	github.com/VictoriaMetrics/fastcache v1.10.0 // indirect
	github.com/VictoriaMetrics/fasthttp v1.1.0 // indirect
	github.com/VictoriaMetrics/metrics v1.18.1 // indirect
	github.com/VictoriaMetrics/metricsql v0.41.0 // indirect
	github.com/benbjohnson/clock v1.3.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/buger/jsonparser v1.1.1 // indirect
	github.com/cenkalti/backoff/v4 v4.1.1 // indirect
	github.com/cespare/xxhash v1.1.0 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/cockroachdb/errors v1.8.1 // indirect
	github.com/cockroachdb/logtags v0.0.0-20190617123548-eb05cc24525f // indirect
	github.com/cockroachdb/redact v1.0.8 // indirect
	github.com/cockroachdb/sentry-go v0.6.1-cockroachdb.2 // indirect
	github.com/coocood/freecache v1.2.1 // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd/v22 v22.3.2 // indirect
	github.com/cznic/mathutil v0.0.0-20181122101859-297441e03548 // indirect
	github.com/danjacques/gofslock v0.0.0-20191023191349-0a45f885bc37 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgraph-io/ristretto v0.1.1 // indirect
	github.com/dgryski/go-farm v0.0.0-20200201041132-a6ae2369ad13 // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/fogleman/gg v1.3.0 // indirect
	github.com/gin-contrib/sse v0.1.0 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/go-playground/locales v0.14.0 // indirect
	github.com/go-playground/universal-translator v0.18.0 // indirect
	github.com/go-playground/validator/v10 v10.10.0 // indirect
	github.com/go-resty/resty/v2 v2.6.0 // indirect
	github.com/goccy/go-json v0.9.11 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang-jwt/jwt/v4 v4.4.2 // indirect
	github.com/golang/freetype v0.0.0-20170609003504-e2365dfdc4a0 // indirect
	github.com/golang/glog v1.0.0 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/google/btree v1.1.2 // indirect
	github.com/google/flatbuffers v2.0.0+incompatible // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/gorilla/websocket v1.4.2 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.16.0 // indirect
	github.com/ianlancetaylor/demangle v0.0.0-20210905161508-09a460cdf81d // indirect
	github.com/jonboulle/clockwork v0.2.2 // indirect
	github.com/joomcode/errorx v1.0.1 // indirect
	github.com/jpillora/backoff v1.0.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.15.13 // indirect
	github.com/kr/pretty v0.3.0 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/leodido/go-urn v1.2.1 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/mattn/go-isatty v0.0.17 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.4 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/mwitkow/go-conntrack v0.0.0-20190716064945-2f068394615f // indirect
	github.com/opentracing/basictracer-go v1.0.0 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/pelletier/go-toml/v2 v2.0.1 // indirect
	github.com/pingcap/errors v0.11.5-0.20221009092201-b66cddb77c32 // indirect
	github.com/pingcap/failpoint v0.0.0-20220423142525-ae43b7f4e5c3 // indirect
	github.com/pingcap/tidb/parser v0.0.0-20230302035309-11148627b1e6 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
	github.com/prometheus/client_model v0.3.0 // indirect
	github.com/prometheus/procfs v0.9.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20200410134404-eec4a21b6bb0 // indirect
	github.com/rogpeppe/go-internal v1.8.0 // indirect
	github.com/shirou/gopsutil/v3 v3.22.12 // indirect
	github.com/sirupsen/logrus v1.9.0 // indirect
	github.com/soheilhy/cmux v0.1.5 // indirect
	github.com/stathat/consistent v1.0.0 // indirect
	github.com/stretchr/objx v0.5.0 // indirect
	github.com/tiancaiamao/gp v0.0.0-20221230034425-4025bc8a4d4a // indirect
	github.com/tikv/client-go/v2 v2.0.5-0.20230202101145-8fd09cd88cce // indirect
	github.com/tikv/pd v1.1.0-beta.0.20230202094356-18df271ce57f // indirect
	github.com/tikv/pd/client v0.0.0-20230202094356-18df271ce57f // indirect
	github.com/tklauser/go-sysconf v0.3.11 // indirect
	github.com/tklauser/numcpus v0.6.0 // indirect
	github.com/tmc/grpc-websocket-proxy v0.0.0-20201229170055-e5319fda7802 // indirect
	github.com/twmb/murmur3 v1.1.3 // indirect
	github.com/uber/jaeger-client-go v2.28.0+incompatible // indirect
	github.com/uber/jaeger-lib v2.4.1+incompatible // indirect
	github.com/ugorji/go/codec v1.2.7 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/valyala/fastjson v1.6.3 // indirect
	github.com/valyala/fastrand v1.1.0 // indirect
	github.com/valyala/fasttemplate v1.2.1 // indirect
	github.com/valyala/histogram v1.2.0 // indirect
	github.com/valyala/quicktemplate v1.7.0 // indirect
	github.com/vmihailenco/msgpack/v5 v5.3.5 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	github.com/xiang90/probing v0.0.0-20190116061207-43a291ad63a2 // indirect
	github.com/yusufpapurcu/wmi v1.2.2 // indirect
	go.etcd.io/bbolt v1.3.6 // indirect
	go.etcd.io/etcd/client/v2 v2.305.7 // indirect
	go.etcd.io/etcd/pkg/v3 v3.5.7 // indirect
	go.etcd.io/etcd/raft/v3 v3.5.7 // indirect
	go.etcd.io/etcd/server/v3 v3.5.7 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.25.0 // indirect
	go.opentelemetry.io/otel v1.0.1 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.0.1 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.0.1 // indirect
	go.opentelemetry.io/otel/sdk v1.0.1 // indirect
	go.opentelemetry.io/otel/trace v1.0.1 // indirect
	go.opentelemetry.io/proto/otlp v0.9.0 // indirect
	go.uber.org/multierr v1.9.0 // indirect
	golang.org/x/crypto v0.5.0 // indirect
	golang.org/x/exp v0.0.0-20221023144134-a1e5550cf13e // indirect
	golang.org/x/image v0.5.0 // indirect
	golang.org/x/oauth2 v0.3.0 // indirect
	golang.org/x/sync v0.1.0 // indirect
	golang.org/x/sys v0.5.0 // indirect
	golang.org/x/text v0.7.0 // indirect
	golang.org/x/time v0.3.0 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20221202195650-67e5cbc046fd // indirect
	google.golang.org/protobuf v1.28.1 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	sigs.k8s.io/yaml v1.3.0 // indirect
)

replace github.com/pingcap/tidb/parser => github.com/pingcap/tidb/parser v0.0.0-20230228095709-06fd2eb6587e

replace github.com/genjidb/genji/engine/badgerengine => github.com/crazycs520/genji/engine/badgerengine v0.12.1-0.20220328082424-727a2d089bde

replace github.com/dgraph-io/ristretto => github.com/dgraph-io/ristretto v0.1.1-0.20220403145359-8e850b710d6d
