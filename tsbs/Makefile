BIN_NAME ?= tsbsloader
VERSION ?= dev
IMAGE_NAME ?= $(BIN_NAME):$(VERSION)
DOCKER_ID_USER ?= timeplus

DATE=$(shell gdate -Iseconds)
COMMIT=$(shell git rev-parse --short HEAD)
FULLNAME=$(DOCKER_ID_USER)/${BIN_NAME}:dev_${COMMIT}

.PHONY: gen docker gen_mock

generate-devops-data:
	docker run -it --rm --entrypoint /tsbs_generate_data timeplus/tsbs:20220629 \
	--use-case="devops" --seed=123 --scale=10 \
    --timestamp-start="2016-01-01T00:00:00Z" \
    --timestamp-end="2016-01-02T00:00:00Z" \
    --log-interval="60s" --format="timescaledb" \
	| gzip > ./data/devops-data.gz

generate-iot-data:
	docker run -it --rm --entrypoint /tsbs_generate_data timeplus/tsbs:20220629 \
	--use-case="iot" --seed=123 --scale=10 \
    --timestamp-start="2016-01-01T00:00:00Z" \
    --timestamp-end="2016-01-02T00:00:00Z" \
    --log-interval="60s" --format="timescaledb" \
	| gzip > ./data/iot-data.gz


generate-queries:
	docker run -it --rm --entrypoint /tsbs_generate_queries timeplus/tsbs:20220629 \
		--use-case="devops" --seed=123 --scale=10 \
		--timestamp-start="2016-01-01T00:00:00Z" \
		--timestamp-end="2016-01-05T00:00:00Z" \
		--queries=1 --query-type="groupby-orderby-limit" --format="timescaledb" \
		> ./query/devops-queries

build:
	go build 

run:
	go run main.go

runm:
	go run main.go --metrics-schema multiple

run-iot:
	go run main.go -f ./data/iot-data.gz -m iot -r

docker: Dockerfile
	docker build -t $(IMAGE_NAME) .

docker_run:
	docker run $(IMAGE_NAME)

push:
	docker tag $(IMAGE_NAME) $(FULLNAME)
	docker tag $(IMAGE_NAME) $(DOCKER_ID_USER)/${BIN_NAME}:latest
	docker push $(FULLNAME)
	docker push $(DOCKER_ID_USER)/${BIN_NAME}:latest