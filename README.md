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
  path = "log"
  
  # Log level: DEBUG, INFO, WARN, ERROR
  level = "INFO"
  
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

## Reload Config

```shell
$ bin/ng-monitoring-server --config config/config.toml.example

# Another shell session
$ pkill -SIGHUP ng-monitoring-server
```

<!-- VERSION_PLACEHOLDER: v8.5.5 -->

<!-- VERSION_PLACEHOLDER: v8.2.0-alpha -->