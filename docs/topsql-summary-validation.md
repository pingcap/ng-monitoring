# Top SQL Summary validation

Validation date: 2026-10-19. Scope: PR #373 against `main` at
`fce47f676635a64f79f7d46fe3c5af0895ed5102`, Go 1.25.14, embedded VM v1.110.5.
These results cover the query implementation, not customer incident attribution
or deployment acceptance.

## Regression results

| Scenario | Test and result |
| --- | --- |
| Writes after ranking | `TestSummaryLateSamplesKeepInitialSelection`: selection remains fixed; later curves include new plans/data. |
| Writes after all selected batches, before total | `TestSummaryWritesBetweenTopAndTotal`: real VM, Top 300, six network batches and two auxiliary batches. No writes: Other=10. Selected SQL receives 20: residual Other=30 although unselected usage remains 10. Unselected SQL receives 20: Other=30, matching its usage. Curves and execution statistics both cover these cases. Conservation alone does not establish attribution. |
| Negative residual | `TestNegativeOtherDifferenceFailsQuery`: controlled inconsistent responses with selected=10 and later total=9. Both the public Summary path and auxiliary fallback fail the query. This simulates inconsistent reads; it does not claim ordinary nonnegative appends produce negative values. No clamping or floating-point tolerance is applied. Equal total and selected values produce zero successfully. |
| More than 100 regex candidates | `TestSummaryLargeCPURegexp`: 300 SQLs, two plans, two tables and two timestamps. Top 100/101/150/200 execute actual positive and negative VM filters, with the entire requested candidate set in each expression. SQL/plan results, CPU totals and auxiliary totals are checked independently. VM's `GetOrValuesPromRegex` returns literals at 100 and no literal list above 100. This confirms extraction eligibility; it does not assert the optimizer must choose a particular storage-index path. |
| Expression-construction cost | `TestSplitTopSQLDigestsLimitsExpressionConstruction`: 500 digests previously caused 502 full expression builds. Binary search keeps construction below 64 while preserving the partition and query byte limit. Existing query-length, empty-selection, sparse-metric and cancellation regressions remain enabled. |

The defined read contract remains a fixed selection with independent subsequent
reads. It is not a common snapshot. In particular, the positive residual bias
above is an observed limitation requiring review, not a consistency fix.
Negative residuals now return the existing query-error response (HTTP 503),
rather than silently losing curve points or returning negative auxiliary rates.

## Paired query benchmark

`BenchmarkSummaryFanout` imports the same fixture into real embedded VM on both
revisions: 300 SQLs × two plans × two tables × 20 samples, with CPU, network,
logical IO and all five auxiliary metric families. Metadata is a no-IO stub,
so timings exclude SQL/plan storage latency and the HTTP proxy chain. The old
revision uses its original Summary implementation; the only benchmark adapter
removes the added context argument from that call.

Runs are sequential on the same darwin/arm64 machine with `GOMAXPROCS=1` and
`GOMEMLIMIT=2GiB`, 200ms per benchmark, three repetitions. Values below are
medians, not statistical latency guarantees. These settings bound Go scheduling
and provide a soft Go memory limit; they are **not** a container CPU quota or
hard memory limit. VM result caching is disabled by the query implementations;
other caches may be warm.

`VM JSON` counts bytes generated into the existing VM response buffers across
all queries in one Summary, without copying or decoding them for measurement.
It is not network traffic. `Allocated` is Go allocation volume per operation,
not live heap or peak RSS.

| Metric / Top | Old ms | PR ms | Old VM JSON KiB | PR VM JSON KiB | Old allocated MiB | PR allocated MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| cpu / 5 | 29.35 | 10.07 | 1816.9 | 14.6 | 18.79 | 3.42 |
| cpu / 100 | 28.58 | 14.69 | 1816.9 | 250.2 | 17.95 | 8.38 |
| cpu / 101 | 29.03 | 11.74 | 1816.9 | 252.7 | 17.95 | 8.42 |
| cpu / 150 | 28.49 | 15.17 | 1816.9 | 373.9 | 17.64 | 11.84 |
| cpu / 200 | 28.22 | 17.63 | 1816.9 | 496.9 | 17.26 | 14.28 |
| network / 5 | 32.68 | 21.18 | 1817.0 | 14.6 | 21.50 | 20.58 |
| network / 100 | 31.49 | 30.43 | 1817.0 | 250.3 | 20.66 | 29.93 |
| network / 101 | 31.13 | 30.21 | 1817.0 | 252.8 | 20.66 | 30.04 |
| network / 150 | 30.90 | 36.61 | 1817.0 | 374.1 | 20.35 | 35.80 |
| network / 200 | 31.41 | 41.98 | 1817.0 | 497.3 | 19.97 | 40.66 |
| logical_io / 5 | 34.51 | 21.58 | 1817.0 | 14.6 | 21.52 | 20.65 |
| logical_io / 100 | 32.41 | 30.16 | 1817.0 | 250.3 | 20.76 | 30.02 |
| logical_io / 101 | 32.18 | 30.46 | 1817.0 | 252.8 | 20.68 | 30.13 |
| logical_io / 150 | 32.68 | 35.79 | 1817.0 | 374.1 | 20.37 | 35.86 |
| logical_io / 200 | 31.63 | 41.87 | 1817.0 | 497.3 | 19.99 | 40.76 |

There is no observed latency cliff at 101 candidates in this fixture. CPU Top 5
and Top 200 improve over the old implementation. Composite metrics at Top 150
and 200 remain slower and allocate more than the old implementation despite
returning less JSON. In particular, Network/Logical IO Top 200 are about 42ms
versus 31–32ms old. These are measured performance trade-offs; deployment
performance acceptance has not passed for all dimensions.

The initial paired run also exposed quadratic expression construction. In a
CPU Top 200 profile, `splitTopSQLDigests` accounted for 1.74s / 3.79s of sampled
CPU (including the benchmark process setup). Binary search reduces that to
0.37s / 3.69s in the follow-up profile. CPU Top 200 changes from about 55ms before
this construction fix to 17.6ms afterwards. These profile proportions describe
this harness, not production CPU attribution.

## Reproduction

Run the focused regressions:

```sh
GOTOOLCHAIN=go1.25.14 go test ./component/topsql/query -run 'Test(NegativeOther|Subtract|SplitTopSQLDigests)' -count=1 -v
GOTOOLCHAIN=go1.25.14 go test ./component/topsql -run 'TestTopSQL/(TestSummaryWritesBetweenTopAndTotal|TestSummaryLargeCPURegexp)' -count=1 -v
```

Run the query benchmark without starting the integration suite:

```sh
env GOTOOLCHAIN=go1.25.14 GOMAXPROCS=1 GOMEMLIMIT=2GiB go test ./component/topsql -run '^$' -bench '^BenchmarkSummaryFanout$' -benchtime=200ms -count=3
```

For the old baseline, create a detached worktree at the commit above, copy only
`component/topsql/summary_benchmark_test.go` into it, and remove the leading
`context.Background()` argument from its `dq.Summary` call. Run the identical
benchmark command there. No baseline implementation or dependencies are changed.

CPU profile command:

```sh
env GOTOOLCHAIN=go1.25.14 GOMAXPROCS=1 GOMEMLIMIT=2GiB go test ./component/topsql -run '^$' -bench '^BenchmarkSummaryFanout$/cpu/top200$' -benchtime=2s -count=1 -cpuprofile /tmp/topsql-cpu.pprof -o /tmp/topsql-profile.test
GOTOOLCHAIN=go1.25.14 go tool pprof -top -cum /tmp/topsql-profile.test /tmp/topsql-cpu.pprof
```

## Dashboard → NGM HTTP check (nginx skipped)

The optional harness in `tools/dashboard-proxy-check/` runs the actual Dashboard
`NgmProxy.Route` over loopback HTTP to NGM's actual Summary service, VM and
SQLite metadata store. Dashboard is pinned to
`44f05e913c1b249182d51e34c7b289a1761a97d1`; its proxy source differs from the
previously inspected `8cfdaf48f4b1458a13ab12ba302d17f624ce0494` only in the copyright
year. NGM behavior was tested at `f02d84c`. No Dashboard production source or
NGM dependency version is changed. The script creates a temporary Dashboard
worktree, adds only a test, then cleans up the test servers/worktree.

Both normal and race-instrumented runs passed. The race run instruments both
processes. Representative elapsed times below are observations, not latency
assertions for a deployment.

| Scenario | Observed result |
| --- | --- |
| Normal query | HTTP 200, SQL text from real SQLite and CPU=100 from VM; 13 VM calls, two metadata reads. |
| Response slower than Dashboard connect timeout | Dashboard connect timeout=1s; first VM boundary delayed 1.2s; query still returns HTTP 200 at about 1.21s. The connect timeout does not bound response waiting. |
| Client cancel | Cancellation reaches NGM through the actual proxy; one VM call, no metadata reads, no later query. NGM reports context canceled and does not decode deliberately invalid VM output. |
| NGM budget | Configured 150ms total budget returns HTTP 503 through Dashboard at about 153–156ms. One VM call, no metadata reads; forwarded VM timeout is positive and at most 150ms. |
| Delayed execution boundary | Client cancel reaches NGM while the VM seam is deliberately held. NGM finishes only after release, then skips decoding and subsequent work. This proves the cooperative boundary, not interruption of an actual VM evaluator. |

Queued/delayed behavior is injected at the VM-handler boundary. Only normal and
slow-success cases evaluate the real VM fixture. Discovery is seeded into the
proxy cache; login, feature guards and PD/etcd discovery are outside the harness.
`context_done` in the diagnostic output also becomes true on normal completion
because Summary defers cancellation of its child context; it is not by itself
evidence of a client disconnect. The cancel cases additionally check the client
error, NGM error body, call counts and, for delayed execution, context propagation
before NGM finishes.

Run from the NGM repository root with a local Dashboard checkout containing the
pinned revision (the optional second argument changes that revision):

```sh
bash tools/dashboard-proxy-check/run.sh /path/to/tidb-dashboard 44f05e913c1b249182d51e34c7b289a1761a97d1 /tmp/proxy-results
PROXY_CHECK_RACE=1 bash tools/dashboard-proxy-check/run.sh /path/to/tidb-dashboard 44f05e913c1b249182d51e34c7b289a1761a97d1 /tmp/proxy-race-results
```

The run requires neither nginx nor Docker, and does not recreate a customer
nginx 504. Gateway timeout configuration remains unverified by this check.

## Repository CI qualification

The initial local full repository run passed. Cloud CI for supplemental commit
`f02d84c` passed format/lint and all Top SQL/config packages, but timed out after
10 minutes in `conprof/store.TestStoreProfileStatus`, blocked in Genji
`Database.Close` on its transaction mutex. The same stuck close was reproduced
locally both on this PR and on the unmodified `main` baseline `fce47f6`; that
package and the Genji adapter are unchanged by this PR. A 50-iteration baseline
run passed, while a later 500-iteration run timed out with the matching stack,
so a passing single run does not establish stability.

Baseline reproduction, from a clean worktree at `fce47f6`:

```sh
GOTOOLCHAIN=go1.25.14 go test ./component/conprof/store -run '^TestStoreProfileStatus$' -count=500 -timeout=30s
```

This is a separate repository test instability, not a successful full cloud CI
result. No conprof/Genji production change is included. Check the PR's current
checks for the latest run status.

## Outstanding acceptance

- Pressure tests under real CPU quotas and hard memory limits, with concurrent
  refreshes and representative cardinality; record latency, peak memory and errors.
- nginx timeout reproduction is explicitly outside the requested proxy-check scope.
- Review whether the observed residual attribution is acceptable and whether
  large-Top composite metrics require further optimization or a narrower guarantee.

No isolated Linux/container environment with CPU quotas and hard memory limits
was available. The local benchmark is not reported as completion of those
pressure tests. Dashboard → NGM is now checked with native Go processes;
nginx was deliberately excluded from the requested scope.
