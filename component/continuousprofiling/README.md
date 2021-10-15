# HTTP API

```shell
# get current config
curl http://0.0.0.0:10092/config

# modify config
curl -X POST -d '{"continuous_profiling": {"enable": false,"profile_seconds":6,"interval_seconds":11}}' http://0.0.0.0:10092/config

# estimate size profile data size
curl http://0.0.0.0:10092/continuous-profiling/estimate-size\?days\=3

# query profile list
curl -X POST -d '{"begin_time":1634182783, "end_time":1634182883}' http://0.0.0.0:10092/continuous-profiling/list

# query profile list with specified targets
curl -X POST -d '{"begin_time":1634182783, "end_time":1634182883, "targets": [{"component": "tidb", "kind": "profile", "address": "10.0.1.21:10081"}]}' http://0.0.0.0:10092/continuous-profiling/list


# Download profile
curl -X POST -d '{"begin_time":1634182783, "end_time":1634182883}' http://0.0.0.0:10092/continuous-profiling/download > download.zip
```
