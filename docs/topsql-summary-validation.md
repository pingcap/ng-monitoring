# Top SQL Summary validation

Validation date: 2026-10-19. Scope: PR #373 against `main` at
`fce47f676635a64f79f7d46fe3c5af0895ed5102`, Go 1.25.14, embedded VM v1.110.5.
These results cover the query implementation, not customer incident attribution
or deployment acceptance.

## Regression results

| Scenario | Test and result |
| --- | --- |
| Writes after ranking | `TestSummaryLateSamplesKeepInitialSelection`: selection remains fixed; later curves include new plans/data. |
| Writes after all selected batches, before total | `TestSummaryWritesBetweenTopAndTotal`: real VM, Top 300, three network batches and two auxiliary batches. No writes: Other=10. Selected SQL receives 20: residual Other=30 although unselected usage remains 10. Unselected SQL receives 20: Other=30, matching its usage. Curves and execution statistics both cover these cases. Conservation alone does not establish attribution. |
| Negative residual | `TestNegativeOtherDifferenceFailsQuery`: controlled inconsistent responses with selected=10 and later total=9. Both the public Summary path and auxiliary fallback fail the query. This simulates inconsistent reads; it does not claim ordinary nonnegative appends produce negative values. No clamping or floating-point tolerance is applied. Equal total and selected values produce zero successfully. |
| More than 100 regex candidates | `TestSummaryLargeCPURegexp`: 300 SQLs, two plans, two tables and two timestamps. Top 100/101/150/200 execute actual positive and negative VM filters, with the entire requested candidate set in each expression. SQL/plan results, CPU totals and auxiliary totals are checked independently. VM's `GetOrValuesPromRegex` returns literals at 100 and no literal list above 100. This confirms extraction eligibility; it does not assert the optimizer must choose a particular storage-index path. |
| Composite incremental aggregation | `TestSummaryCompositeIncrementalAggregation`: real VM trace verifies two incremental sums for ranking, selected curves and Other across TiKV/TiDB network and logical IO. Disjoint tables, one-sided plans/windows and native Other retain exact values. With `MaxSeries=4`, each separate scan succeeds while a combined metric-name selector fails on eight matching raw series. |
| Unlimited table aggregation | `TestSummaryUnlimitedTableAggregation`: CPU/network/logical IO, `top=0` and `top=-1`, multiple tables/plans and sparse data. Tables sum to one point per timestamp (30, 7), rather than duplicate curve points. |
| Expression-construction cost | `TestSplitTopSQLDigestsLimitsExpressionConstruction`: 500 digests previously caused 502 full expression builds. Binary search keeps construction below 64 while preserving the partition and query byte limit. Existing query-length, empty-selection, sparse-metric and cancellation regressions remain enabled. |

The defined read contract remains a fixed selection with independent subsequent
reads. It is not a common snapshot. In particular, the positive residual bias
above is an observed limitation requiring review, not a consistency fix.
Negative residuals now return the existing query-error response (HTTP 503),
rather than silently losing curve points or returning negative auxiliary rates.

## Paired query benchmark

Latest runs compare original `main` at `fce47f6`, the previously reviewed PR at
`a06ec7c`, and this supplemental revision. Each uses the same
`BenchmarkSummaryFanout` fixture: real embedded VM v1.110.5, 300 SQLs × two plans
× two tables × 20 samples, CPU/network/logical IO and five auxiliary families.
Metadata is a no-IO stub, so timings exclude metadata storage and HTTP proxies.
Only the added context argument is removed for the original main benchmark.

Runs are sequential on the same darwin/arm64 machine with Go 1.25.14,
`GOMAXPROCS=1`, soft `GOMEMLIMIT=2GiB`, 200ms per case, three repetitions.
Values are medians of short local runs with visible timing variation, not
statistical latency guarantees, container CPU quotas or hard memory limits.
VM result caching is disabled by the queries; other caches may be warm.
`Allocated` is cumulative Go allocation per request, not live heap or peak RSS.

| Metric / Top | Main ms | a06ec7c ms | New ms | Main allocated MiB | a06ec7c allocated MiB | New allocated MiB | New VM JSON KiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| cpu / top5 | 30.19 | 7.17 | 6.23 | 18.79 | 3.42 | 3.43 | 14.6 |
| cpu / top100 | 29.01 | 13.41 | 11.90 | 17.95 | 8.38 | 8.43 | 250.2 |
| cpu / top101 | 29.41 | 13.16 | 12.40 | 17.95 | 8.42 | 8.47 | 252.7 |
| cpu / top150 | 31.41 | 16.74 | 15.44 | 17.64 | 11.84 | 11.92 | 373.9 |
| cpu / top200 | 28.79 | 18.53 | 24.24 | 17.26 | 14.28 | 14.38 | 496.9 |
| network / top5 | 32.22 | 21.67 | 10.17 | 21.50 | 20.58 | 4.55 | 14.6 |
| network / top100 | 31.55 | 31.42 | 16.23 | 20.66 | 29.94 | 10.24 | 250.2 |
| network / top101 | 31.50 | 31.77 | 15.35 | 20.66 | 30.04 | 10.29 | 252.7 |
| network / top150 | 31.52 | 38.12 | 22.14 | 20.35 | 35.78 | 14.67 | 374.0 |
| network / top200 | 33.57 | 50.06 | 24.90 | 19.97 | 40.67 | 18.16 | 497.1 |
| logical_io / top5 | 32.87 | 30.44 | 9.43 | 21.52 | 20.65 | 4.64 | 14.6 |
| logical_io / top100 | 37.19 | 33.92 | 16.60 | 20.96 | 30.02 | 10.25 | 250.2 |
| logical_io / top101 | 32.36 | 30.82 | 14.49 | 20.68 | 24.22 | 10.29 | 252.7 |
| logical_io / top150 | 32.75 | 36.77 | 19.27 | 20.37 | 35.86 | 14.67 | 374.0 |
| logical_io / top200 | 32.60 | 40.77 | 22.66 | 19.99 | 40.72 | 18.23 | 497.1 |

Original main generated about 1817 KiB of VM JSON for each case. This is bytes
produced into VM response buffers across all queries, not network traffic.
The new composite queries retain the selected SQL/plan output contract while
reducing intermediate allocation. Network Top 200 changes from 50.06ms / 40.67MiB
at a06ec7c to 24.90ms / 18.16MiB; Logical IO changes from 40.77ms / 40.72MiB to
22.66ms / 18.23MiB. Both also beat original main in this fixture, so the previously
measured large-Top composite regression is removed here. This does not establish
production-cardinality or hard-resource-limit behavior. CPU expressions are
unchanged for finite Top; the varying CPU timings are not claimed as an added
benefit of this supplemental change.

## Why this optimization and what remains worthwhile

Composite queries now aggregate each metric before combining it, for example:

```promql
sum(
  sum(sum_over_time(network_in_bytes{...}[6])) by (sql_digest, plan_digest),
  sum(sum_over_time(network_out_bytes{...}[6])) by (sql_digest, plan_digest)
) by (sql_digest, plan_digest)
```

Each inner sum has VM's incremental-aggregation shape; the outer MetricsQL
multi-argument sum merges the small outputs, including a missing counterpart.
This replaces raw-series zero-fill/vector joins and repeats the digest matcher
twice instead of four times. Network Top 100 now needs one selected batch rather
than two; Top 200 needs two rather than four. The existing query-byte budget and
Other subtraction fallback remain in effect. Unlimited queries reuse the same
aggregation and now merge tables before returning curves.

A single selector matching both metric names would be shorter, but would count
both families against one scan's series limit. The real VM limit regression
above rejects that design; no limit is increased. The normal/evaluation trace
assertions, not expression inspection alone, establish incremental execution.
The Network Top 200 CPU profile before this change contains substantial binary
operation work; the follow-up profile shifts toward auxiliary statistics.
These profiles include benchmark setup and are not production CPU attribution.

| Option | ROI decision |
| --- | --- |
| Aggregate composite sides before merging | Implemented: one shared query builder, measured latency/allocation improvement, no dependency or ingestion changes. |
| Reuse aggregation for unlimited curves | Implemented: removes the known cross-table duplicate timestamps through the same query path. Still returns every digest. |
| Measure under hard quotas and concurrent refresh | Next validation priority; needed for deployment acceptance and peak-memory/error-rate evidence. |
| Batch/cache SQL and plan metadata or combine auxiliary queries | Profile a workload with real metadata IO first. This benchmark stubs that IO; combining metric selectors can also change scan limits and increase expression size. No speculative implementation here. |
| Parallel VM requests, cross-request cache, ingestion/storage redesign | Deferred: unmeasured benefit versus more memory/queue pressure, cache freshness rules, or cross-component compatibility work. |

## Reproduction

Run the focused regressions:

```sh
GOTOOLCHAIN=go1.25.14 go test ./component/topsql/query -run 'Test(NegativeOther|Subtract|SplitTopSQLDigests)' -count=1 -v
GOTOOLCHAIN=go1.25.14 go test ./component/topsql -run 'TestTopSQL/(TestSummaryWritesBetweenTopAndTotal|TestSummaryLargeCPURegexp|TestSummaryCompositeIncrementalAggregation|TestSummaryUnlimitedTableAggregation)' -count=1 -v
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
- Review acceptance of residual attribution and negative-residual HTTP 503 behavior.
- Verify the latest composite gains at representative production cardinality;
  the small-fixture regression is removed, not a deployment guarantee.

No isolated Linux/container environment with CPU quotas and hard memory limits
was available. The local benchmark is not reported as completion of those
pressure tests. Dashboard → NGM is now checked with native Go processes;
nginx was deliberately excluded from the requested scope.
