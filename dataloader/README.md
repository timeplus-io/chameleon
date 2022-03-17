# Introduction

This (dataload) tool aims to ingest IoT [JSON](https://github.com/timeplus-io/chameleon/blob/main/dataloader/models/metric.go) data to external streaming stores : Redpanda and Kafka and then we streaming query the JSON data directly from Kafka / Redpanda in realtime by using Timeplus. The intial goals are finding how Timeplus works in this scenario.


# Workflow

![Test Deployment](deployment.png)
dataload ingest data -> Kafka / Redpanda <- Streaming Query in Timeplus


# Ingest Details

In [config.yml](https://github.com/timeplus-io/chameleon/blob/main/dataloader/config/config.yml), we can tune the batch size, concurrency etc when loading the data to Kafka / Redpanda. We will use different batch size and concurrency to ingest data to see how everything works.
In order to test the end to end latency, this dataloader ingests a speicial header called `_tp_time` as the event timestamp to every Kafka/Redpanda record.  This special header in Kafka / Redpanda record is one of the speicial metadata Timeplus can honor.

# Query Details

In Timeplus, we can run the following query statement to caculate the throughput (eps, a.k.a event per second) and latency.

```
SELECT p90(_tp_process_time - _tp_time, 0.9), avg(_tp_process_time - _tp_time) 
FROM device_utils WHERE temperature > 30
```

To be simple, query latency is defined as `_tp_process_time - _tp_time`, where `_tp_time` is where data entering Kafka/Redpanda, and `_tp_process_time` is where the data result is processed and ready to deliver

# Quick Start 

run `go build cmd/dataloader.go` or `make build` , and the run `dataloader --config=<your_config_file>` to load data to configured kafka or redpanda broker 

# Quick Start with Docker

1. run `make docker` to build a docker image locally
2. go to `docker/kafka` or `docker/redpanda` and then run `make run` or `docker-compose up -d`to start the test data loading
3. run `make consume` to insepct generated data stream
4. run `make stop` or `docker-compose down` to stop data geneating

# Note

Please note as the data loader will generating large volume of data and might use a lot of disk space, please make sure stop it or prune the disk space when required.

