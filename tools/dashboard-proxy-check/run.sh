#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 /path/to/tidb-dashboard [dashboard-ref] [output-directory]" >&2
  exit 2
fi
ngm_root=$(git rev-parse --show-toplevel)
dashboard_root=$(cd "$1" && pwd)
dashboard_ref=${2:-44f05e913c1b249182d51e34c7b289a1761a97d1}
output_dir=${3:-$(mktemp -d /tmp/ngm-proxy-results.XXXXXX)}
mkdir -p "$output_dir"
output_dir=$(cd "$output_dir" && pwd)
proxy_tmp=$(mktemp -d /tmp/ngm-dashboard-proxy.XXXXXX)
fixture_pid=""
cleanup() {
  if [[ -n "$fixture_pid" ]]; then
    kill -TERM "$fixture_pid" 2>/dev/null || true
    wait "$fixture_pid" 2>/dev/null || true
  fi
  if [[ -d "$proxy_tmp/dashboard" ]]; then
    rm -f "$proxy_tmp/dashboard/pkg/apiserver/utils/ngm_proxy_validation_test.go"
    git -C "$dashboard_root" worktree remove "$proxy_tmp/dashboard"
  fi
  rm -rf "$proxy_tmp"
}
trap cleanup EXIT
export GOTOOLCHAIN=go1.25.14
cp "$ngm_root/tools/dashboard-proxy-check/ngm_fixture.go.txt" "$proxy_tmp/main.go"
gofmt -w "$proxy_tmp/main.go"
if [[ "${PROXY_CHECK_RACE:-0}" == "1" ]]; then
  go build -race -o "$proxy_tmp/ngm-fixture" "$proxy_tmp/main.go"
else
  go build -o "$proxy_tmp/ngm-fixture" "$proxy_tmp/main.go"
fi
git -C "$dashboard_root" worktree add --detach "$proxy_tmp/dashboard" "$dashboard_ref"
cp "$ngm_root/tools/dashboard-proxy-check/dashboard_proxy_test.go.txt" "$proxy_tmp/dashboard/pkg/apiserver/utils/ngm_proxy_validation_test.go"
gofmt -w "$proxy_tmp/dashboard/pkg/apiserver/utils/ngm_proxy_validation_test.go"
"$proxy_tmp/ngm-fixture" -ready "$proxy_tmp/ready.json" -data "$proxy_tmp/data" > "$output_dir/ngm-fixture.log" 2>&1 &
fixture_pid=$!
python3 - "$proxy_tmp/ready.json" "$fixture_pid" <<'PY'
import json,os,sys,time
path,pid=sys.argv[1],int(sys.argv[2])
for _ in range(600):
    os.kill(pid,0)
    try:
        with open(path) as f: json.load(f)
        break
    except (FileNotFoundError,json.JSONDecodeError):time.sleep(.1)
else:raise SystemExit('NGM fixture startup timed out')
PY
(
  cd "$proxy_tmp/dashboard"
  if [[ "${PROXY_CHECK_RACE:-0}" == "1" ]]; then
    NGM_PROXY_READY="$proxy_tmp/ready.json" go test -race ./pkg/apiserver/utils -run '^TestNGMProxyHTTPValidation$' -count=1 -v
  else
    NGM_PROXY_READY="$proxy_tmp/ready.json" go test ./pkg/apiserver/utils -run '^TestNGMProxyHTTPValidation$' -count=1 -v
  fi
) 2>&1 | tee "$output_dir/dashboard-proxy.log"
# Check the fixture exit too: the race detector may report in either process.
kill -TERM "$fixture_pid"
wait "$fixture_pid"
fixture_pid=""
git -C "$ngm_root" rev-parse HEAD > "$output_dir/ngm-head.txt"
git -C "$dashboard_root" rev-parse "$dashboard_ref" > "$output_dir/dashboard-head.txt"
printf 'Results: %s\n' "$output_dir"
