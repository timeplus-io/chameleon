
# Timeplus Chameleon Generator

Chameleon generator is a stream data generation tool which can be used in streaming functional test and performance test. Written in Golang, chameleon generator can generate random data stream based on pre-defined schema, it also provides observer functions which can be used to observe the performance of data analytic systems.

# Quick Run

To run chameleon generator, for example all data stream to [Proton](https://github.com/timeplus-io/proton), 

1. start a Proton instance using docker `docker run -d --name proton -p 8123:8123 -p 8463:8463 ghcr.io/timeplus-io/proton:latest`
2. run a stream data generator by `go run main.go -f ./samples/yaml/proton.yaml`
3. run `docker exec -it proton proton-client` to start a proton client and then run following query `select * from test` , a stream called `test` is created by the generator and continuously generating random events into that stream. the query will return all generated events in real time.

``` shell
select * from test

SELECT
  *
FROM
  test

Query id: 7e8aeb97-eab5-45cd-b64a-8628c1995dcd

┌─────ordertime─┬─orderid─┬─itemid─┬─orderunits─┬─city───────────┬─state───────┬─zipcode─┬───────────────timestamp─┬──────────time─┬─value─┬────────────────_tp_time─┐
│ 1706120709174 │ O570    │ I866   │   5.490351 │ Newark         │ Connecticut │ 36863   │ 2024-01-24 18:25:09.174 │ 1706120709174 │   996 │ 2024-01-24 18:25:09.177 │
│ 1706120709174 │ O323    │ I984   │  3.0044174 │ San Bernardino │ Colorado    │ 71880   │ 2024-01-24 18:25:09.174 │ 1706120709174 │    86 │ 2024-01-24 18:25:09.177 │
│ 1706120709174 │ O658    │ I756   │   6.129027 │ San Francisco  │ Missouri    │ 11499   │ 2024-01-24 18:25:09.174 │ 1706120709174 │   541 │ 2024-01-24 18:25:09.177 │
│ 1706120709174 │ O774    │ I189   │   8.842153 │ Oklahoma       │ Alabama     │ 30266   │ 2024-01-24 18:25:09.174 │ 1706120709174 │   370 │ 2024-01-24 18:25:09.177 │
└───────────────┴─────────┴────────┴────────────┴────────────────┴─────────────┴─────────┴─────────────────────────┴───────────────┴───────┴─────────────────────────┘
┌─────ordertime─┬─orderid─┬─itemid─┬─orderunits─┬─city──────┬─state────────┬─zipcode─┬───────────────timestamp─┬──────────time─┬─value─┬────────────────_tp_time─┐
│ 1706120709174 │ O572    │ I26    │   9.162193 │ Durham    │ Illinois     │ 99955   │ 2024-01-24 18:25:09.174 │ 1706120709174 │   176 │ 2024-01-24 18:25:09.177 │
│ 1706120709174 │ O482    │ I640   │   9.704296 │ Omaha     │ Kansas       │ 95799   │ 2024-01-24 18:25:09.174 │ 1706120709174 │   519 │ 2024-01-24 18:25:09.177 │
│ 1706120709174 │ O733    │ I271   │  7.8437223 │ Oklahoma  │ Illinois     │ 88276   │ 2024-01-24 18:25:09.174 │ 1706120709174 │   850 │ 2024-01-24 18:25:09.177 │
│ 1706120709174 │ O612    │ I89    │   4.526264 │ Oakland   │ Rhode Island │ 85202   │ 2024-01-24 18:25:09.174 │ 1706120709174 │   166 │ 2024-01-24 18:25:09.177 │
└───────────────┴─────────┴────────┴────────────┴───────────┴──────────────┴─────────┴─────────────────────────┴───────────────┴───────┴─────────────────────────┘
```

also, according to the configuration, two observers are collecting the throughput and data latency. the report will be generated locally after obsever finish its observing job.


# Architecture Principles

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
| `batch_size` |  how many events contained in each batch| `16` |
| `concurency` |  how many concurrent go routines is used to generat event| `2` |
| `interval` |  the interval between each iteration in ms| `1000` |
| `interval_delta` |  a random variation of the interval of each iteration , used to simulate interval jitter| `300` |
| `batch_number ` |  how many iterations to run for each goroutine, if not specified, run max int iterations | `1000` |
| `random_event ` |  when set to false, will a fixed event, this is used for performance test where random data is not required  | `true` |
| `fields` | a list of json fields definition |  |

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

Current supported sink types are 

- `timeplus`
- `proton`
- `splunk`
- `kdb`
- `materialize`
- `ksql`
- `kafka`
- `kafka`
- `console`
- `dolpindb`
- `rocketmq`

refer to [samples](./samples) folder for the sink configurerations

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



