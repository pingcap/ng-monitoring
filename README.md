# Next Generation Monitoring Server

## Build

```shell
make
```

## Arguments

```shell
$ bin/ng-monitoring-server --help
  Usage of bin/ng-monitoring-server:
        --address string             TCP address to listen for http connections
        --advertise-address string   tidb server advertise IP
        --config string              config file path
        --log.path string            Log path of ng monitoring server
        --pd.endpoints strings       Addresses of PD instances within the TiDB cluster. Multiple addresses are separated by commas, e.g. --pd.endpoints 10.0.0.1:2379,10.0.0.2:2379
        --retention-period string    Data with timestamps outside the retentionPeriod is automatically deleted
                                     The following optional suffixes are supported: h (hour), d (day), w (week), y (year). If suffix isn't set, then the duration is counted in months (default "1")
        --storage.path string        Storage path of ng monitoring server
pflag: help requested
```

## Config Example

```shell
$ cat config/config.toml.example
  # NG Monitoring Server Configuration.
  
  # Server address.
  address = "0.0.0.0:12020"
  
  advertise-address = "0.0.0.0:12020"
  
  [log]
  # Log path
  # When set, the following log files are written under this directory:
  # - ng.log
  # - service.log
  # - tsdb.log
  # - docdb.log (only when docdb-backend is not "sqlite")
  #
  # Example:
  #   path = "/var/log/ng-monitoring"
  #   => /var/log/ng-monitoring/ng.log
  #   => /var/log/ng-monitoring/service.log
  #   => /var/log/ng-monitoring/tsdb.log
  #   => /var/log/ng-monitoring/docdb.log (only when docdb-backend is not "sqlite")
  #
  # When unset:
  # - ng.log is written to stdout
  # - service.log is written to stdout
  # - tsdb.log is written to <storage.path>/tsdb-log/tsdb.log
  # - docdb.log is written to ./docdb-log/docdb.log (only when docdb-backend is not "sqlite")
  #
  # Example when unset, storage.path = "data", and docdb-backend = "sqlite":
  # - ng.log => stdout
  # - service.log => stdout
  # - tsdb.log => data/tsdb-log/tsdb.log
  # - docdb.log => not generated
  path = "log"
  
  # Log level: DEBUG, INFO, WARN, ERROR
  level = "INFO"

  # Max size per log file in MB before rotation
  max-size = 300

  # Max age in days for rotated logs. 0 means never delete by age
  max-days = 0

  # Maximum number of rotated log files to keep
  max-backups = 10
  
  [pd]
  # Addresses of PD instances within the TiDB cluster. Multiple addresses are separated by commas, e.g. ["10.0.0.1:2379","10.0.0.2:2379"]
  endpoints = ["0.0.0.0:2379"]
  
  [storage]
  # Storage path of ng monitoring server
  path = "data"
  
  [security]
  ca-path = ""
  cert-path = ""
  key-path = ""
```

## Top SQL By Query

For `top > 0`, Summary ranks SQL digests by cumulative usage in the embedded
VictoriaMetrics engine, then fetches the selected SQL/plan curves and an Other
aggregate. Different table series for the same SQL/plan are summed. Network and
logical IO summaries count a missing input/output or read/write side as zero.
Large selections are batched to fit the query length limit; an oversized Other
filter uses the overall total minus the selected totals.

Ranking and subsequent reads do not share a snapshot. The selected SQL set stays
fixed for a request, while late samples can change its curves, plans, and Other.
In the total-minus-selected fallback, samples added to selected SQLs between reads
can appear in the residual Other. A negative residual fails the query instead of
being clamped or silently discarded. See [validation notes](docs/topsql-summary-validation.md)
for the tested behavior and benchmark limits.
Equal usage at the cutoff can select a different SQL than the previous Go-side
tie-breaker. Unlimited requests (`top <= 0`) still fetch all main-metric series.

`[topsql] summary-timeout = "30s"` sets the total By Query request budget, including
all queries and metadata reads. Each VM query receives the remaining budget and
also obeys `tsdb.search-max-query-duration`. Cancellation is cooperative: queued
and subsequent work can stop, but an executing VM operation may return after the
deadline. This setting does not change By Table/DB/Region or metric record APIs.

## Reload Config

```shell
$ bin/ng-monitoring-server --config config/config.toml.example

# Another shell session
$ pkill -SIGHUP ng-monitoring-server
```

<!-- VERSION_PLACEHOLDER: v8.1.0-alpha -->

<!-- VERSION_PLACEHOLDER: v8.2.0-alpha -->
