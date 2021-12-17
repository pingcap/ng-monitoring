# HTTP API

```shell
# get current config
curl http://0.0.0.0:12020/config

# modify config
curl -X POST -d '{"continuous_profiling": {"enable": false,"profile_seconds":6,"interval_seconds":11}}' http://0.0.0.0:12020/config

# estimate size profile data size
curl http://0.0.0.0:12020/continuous_profiling/estimate_size\?days\=3

# query group profiles

curl "http://0.0.0.0:12020/continuous_profiling/group_profiles?begin_time=1634836900&end_time=1654836910"
[
    {
        "ts": 1634836900,
        "profile_duration_secs": 5,
        "state": "success",
        "component_num": {
            "tidb": 1,
            "pd": 1,
            "tikv": 1,
            "tiflash": 0
        }
    },
    {
        "ts": 1634836910,
        "profile_duration_secs": 5,
        "state": "success",
        "component_num": {
            "tidb": 1,
            "pd": 1,
            "tikv": 1,
            "tiflash": 0
        }
    }
]

# query group profile detail.
curl "http://0.0.0.0:12020/continuous_profiling/group_profile/detail?ts=1634836910"
{
    "ts": 1634836910,
    "profile_duration_secs": 5,
    "state": "success",
    "target_profiles": [
        {
            "state": "success",
            "error": "",
            "profile_type": "profile",
            "target": {
                "component": "tikv",
                "address": "10.0.1.21:20180"
            }
        },
        {
            "state": "success",
            "error": "",
            "profile_type": "profile",
            "target": {
                "component": "pd",
                "address": "10.0.1.21:2379"
            }
        },
        {
            "state": "success",
            "error": "",
            "profile_type": "mutex",
            "target": {
                "component": "tidb",
                "address": "10.0.1.21:10080"
            }
        }
    ]
}

# view single profile data
curl "http://0.0.0.0:12020/continuous_profiling/single_profile/view?ts=1634836910&profile_type=profile&component=tidb&address=10.0.1.21:10080" > profile

# view single profile data and specify data type
curl "http://0.0.0.0:12020/continuous_profiling/single_profile/view?ts=1635480630&profile_type=profile&component=tidb&address=10.0.1.21:10080&data_format=protobuf"  > profile

# Download profile
curl "http://0.0.0.0:12020/continuous_profiling/download?ts=1634836910" > d.zip

# Download profile data and specify data type
curl "http://0.0.0.0:12020/continuous_profiling/download?ts=1635480630&data_format=protobuf" > d.zip

# Download single profile data by specify target
curl "http://0.0.0.0:12020/continuous_profiling/download?ts=1639733820&profile_type=profile&component=pd&address=10.0.1.11:21379&data_format=protobuf" > profile.zip
```
