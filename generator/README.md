
# Timeplus Chameleon Generator

Chameleon generator is a stream data generation tool which can be used in streaming functional test and performance test. Written in Golang, chameleon generator can genertor random data stream based on defined schema, it also provide observer functions which can be used to observe the performance of data analytic systems.

# Quick Run

To run chameleon generator, for example splunk, 

1. start a splunk stack from dir `deplyment/splunk` with `docker-compose up`
2. run a stream data generator by `go run main.go -f ./samples/yaml/splunk.yaml`
3. run a observer with `go run main.go -f ./samples/yaml/splunk_ob_latency.yaml`

the report will be generated locally after obsever finish its observing job.


# Architecture Principles

![Timeplus Chaneleon](architecture.png)

There are three parts while using the generator
1. writer, which is composed by multiple go routines to generating streaming data and write to target system
2. target data processing system, which is the taget of observing, which we want to figure out how well it is when performaing specifig data processing tasks
3. observer, which is used to collecting the performance metrics of the target system, using the query capabilities provided by the target data processing system.


# Generating Stream Data

By configuring the `source`, random stream data can be generated, here is a sample source configuration.

```yaml
source:
  batch_size: 512
  concurency: 4
  interval: 1
  fields:
  - name: value
    type: int
    limit:
    - 0
    - 100000
  - name: time
    type: timestamp
    timestamp_format: '2006-01-02 15:04:05.000000'
```

in above sample, a stream with two fields, one is int and the other is timestamp is defined, and generating at specified speed, 1 ms interval, 4 concurency writers and each batch containing 512 events.

here is the definition of all configurations of `source`

| Field Name | Description |Sample Value|
| ----------- | ----------- | ----------- |
| `batch_size` |  how many events contained in each batch| `0` |
| `interval` |  the interval between each iteration in ms| `1000` |
| `interval_delta` |  a random variation of the interval of each iteration , used to simulate interval jitter| `300` |
| `batch_number ` |  how many iterations to run for each goroutine, if not specified, run max int iterations | `1000` |
| `fields` | a list of fields definition |  |

for fields, it contains following attributes

| Field Name | Description |
| ----------- | ----------- | 
| `name` |  name of the field |  |
| `type` |  what types of data to be generated, support `timestamp`,`timestamp_int`, `string`, `int`, `float`, `bool`, `generate`, `regex`
| `range` |  optional for `string`, `int` and `float`, which is list of value that can be generated | 
| `limit` |  optional for `int` and `float`, a list with two values that specify the min/max of the generated data|
| `timestamp_format` |  optional for `timestamp` type, following golang time string format rules| 
| `timestamp_delay_min` |  minimal delay for timestamp in ms| 
| `timestamp_delay_max` |  maximal delay for timestamp in ms| 
| `rule` |  a generation rule in case the `type` is `generate` or `regex`  | 

[gofakeit](github.com/brianvoe/gofakeit) is used to generator random data when the `type` is specified as `generate` or `regex`, for example, `{year}-{month}-{day}` can be set to `rule` to generate a date like `2006-01-02`. 

# Sinks

By configuring `sinks`, we can specify where the stream data is writting to. for example:

``` yaml
sinks:
- type: splunk
  properties:
    hec_address: https://localhost:8088/services/collector/event
    hec_token: abcd1234
```

above configuration defines one sink of splunk with specified hec address and token.  multiple sinks can be defined in the configuration.  

Current supported sink types are `splunk`,`materialize`,`ksql`,`kafka`,`neutron` (neutron is code name of Timeplus API service).  Please refer to the code for properties required for each sink type.

# Observe System Performance

observer configuration defines which metric to observe, for example:

``` yaml
---
name: test
timeout: 120
observer:
  type: splunk
  properties:
    search: search index=main source="my_source" value=9 | eval eventtime=_time |
      eval indextime=_indextime
    time_field: time
    metric: latency
```

above configuration defines a observer to collecting the latency metric of a splunk realtime query.  

three metrics are now implemented which are `latency`, `throughout`,`availability`

- `latency` defines how long it take from event generated to the event is observed by the observer, the unit is ms
- `throughput` defines how many events the observer can observe for specific range of time, the unit is event per second
- `availbilty` defined how fast the observer can observe all the event generated are avialble in the system

the observing metrics result will be written to a report file as `stack_name`_`metrics_name`_report_`timestamp`.csv

# Plugin Development

Chameleon generator is easy to extend to support new stacks, please refer to https://github.com/timeplus-io/chameleon/tree/main/generator/plugins about how to develop a new plugin.



