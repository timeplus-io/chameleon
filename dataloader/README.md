# Introduction

This (dataload) tool aims to ingest IoT [JSON](https://github.com/timeplus-io/timeplus-redpanda-benchmark/blob/master/models/metric.go) data to external streaming stores : Redpanda and Kafka and then we streaming query the JSON data directly from Kafka / Redpanda in realtime by using Timeplus. The intial goals are finding how Timeplus works in this scenario.


# Workflow

dataload ingest data -> Kafka / Redpanda <- Streaming Query in Timeplus


# Ingest Details

In [config.yml](https://github.com/timeplus-io/timeplus-redpanda-benchmark/blob/master/config/config.yml), we can tune the batch size, concurrency etc when loading the data to Kafka / Redpanda. We will use different batch size and concurrency to ingest data to see how everything works.
In order to test the end to end latency, this dataloader ingests a speicial header called `_tp_time` as the event timestamp to every Kafka/Redpanda record.  This special header in Kafka / Redpanda record is one of the speicial metadata Timeplus can honor.

# Query Details

In Timeplus, we can run the following command to caculate the throughput (eps, a.k.a event per second) and altency.

```
...
```
