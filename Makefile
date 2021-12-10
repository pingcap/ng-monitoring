PACKAGE_LIST  := go list ./...| grep -E "github.com/pingcap/ng_monitoring/"
PACKAGE_LIST_TESTS  := go list ./... | grep -E "github.com/pingcap/ng_monitoring/"
PACKAGES  ?= $$($(PACKAGE_LIST))
PACKAGES_TESTS ?= $$($(PACKAGE_LIST_TESTS))
PACKAGE_DIRECTORIES := $(PACKAGE_LIST) | sed 's|github.com/pingcap/ng_monitoring/||'
FILES     := $$(find $$($(PACKAGE_DIRECTORIES)) -name "*.go")
FAIL_ON_STDOUT := awk '{ print } END { if (NR > 0) { exit 1 } }'

LDFLAGS += -X "github.com/pingcap/ng_monitoring/utils/printer.NGMBuildTS=$(shell date -u '+%Y-%m-%d %H:%M:%S')"
LDFLAGS += -X "github.com/pingcap/ng_monitoring/utils/printer.NGMGitHash=$(shell git rev-parse HEAD)"
LDFLAGS += -X "github.com/pingcap/ng_monitoring/utils/printer.NGMGitBranch=$(shell git rev-parse --abbrev-ref HEAD)"

GO              := GO111MODULE=on go
GOBUILD         := $(GO) build
GOTEST          := $(GO) test -p 8



default:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/ng-monitoring-server ./main.go
	@echo Build successfully!

fmt:
	@echo "gofmt (simplify)"
	@gofmt -s -l -w . 2>&1 | $(FAIL_ON_STDOUT)
	@gofmt -s -l -w $(FILES) 2>&1 | $(FAIL_ON_STDOUT)

test:
	@echo "Running test"
	@export log_level=info; export TZ='Asia/Shanghai'; \
	$(GOTEST) -cover $(PACKAGES_TESTS) -coverprofile=coverage.txt