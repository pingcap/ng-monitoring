PACKAGE_LIST  := go list ./...| grep -E "github.com/pingcap/ng-monitoring/"
PACKAGE_LIST_TESTS  := go list ./... | grep -E "github.com/pingcap/ng-monitoring/"
PACKAGES  ?= $$($(PACKAGE_LIST))
PACKAGES_TESTS ?= $$($(PACKAGE_LIST_TESTS))
PACKAGE_DIRECTORIES := $(PACKAGE_LIST) | sed 's|github.com/pingcap/ng-monitoring/||'
FILES     := $$(find $$($(PACKAGE_DIRECTORIES)) -name "*.go")
FAIL_ON_STDOUT := awk '{ print } END { if (NR > 0) { exit 1 } }'
BUILD_TS := $(shell date -u '+%Y-%m-%d %H:%M:%S')
GIT_HASH := $(shell git rev-parse HEAD)
GIT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD)

BUILD_GOEXPERIMENT ?=
BUILD_CGO_ENABLED ?=
BUILD_TAGS ?=
ifeq ("${ENABLE_FIPS}", "1")
	GIT_HASH := $(GIT_HASH) (with fips)
	GIT_BRANCH := $(GIT_BRANCH) (with fips)
	BUILD_TAGS += boringcrypto
	BUILD_GOEXPERIMENT = GOEXPERIMENT=boringcrypto
	BUILD_CGO_ENABLED = CGO_ENABLED=1
endif

LDFLAGS += -X "github.com/pingcap/ng-monitoring/utils/printer.NGMBuildTS=$(BUILD_TS)"
LDFLAGS += -X "github.com/pingcap/ng-monitoring/utils/printer.NGMGitHash=$(GIT_HASH)"
LDFLAGS += -X "github.com/pingcap/ng-monitoring/utils/printer.NGMGitBranch=$(GIT_BRANCH)"

GO              := $(BUILD_GOEXPERIMENT) $(BUILD_CGO_ENABLED) GO111MODULE=on go
GOBUILD         := $(GO) build
GOTEST          := $(GO) test -p 8



default:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -tags '${BUILD_TAGS}' -o bin/ng-monitoring-server ./main.go
	@echo Build successfully!

fmt:
	@echo "gofmt (simplify)"
	@gofmt -s -l -w . 2>&1 | $(FAIL_ON_STDOUT)
	@gofmt -s -l -w $(FILES) 2>&1 | $(FAIL_ON_STDOUT)

test:
	@echo "Running test"
	@export log_level=info; export TZ='Asia/Shanghai'; \
	$(GOTEST) -cover $(PACKAGES_TESTS) -coverprofile=coverage.txt

lint: tools/bin/golangci-lint
	GO111MODULE=on tools/bin/golangci-lint run -v $$($(PACKAGE_DIRECTORIES)) --config .golangci.yml

tools/bin/golangci-lint:
	curl -sfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh| sh -s -- -b ./tools/bin v2.4.0
