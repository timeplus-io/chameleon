
BIN_NAME ?= generator
VERSION ?= dev
IMAGE_NAME ?= $(BIN_NAME):$(VERSION)
DOCKER_ID_USER ?= timeplus

DATE=$(shell gdate -Iseconds)
COMMIT=$(shell git rev-parse --short HEAD)
FULLNAME=$(DOCKER_ID_USER)/${BIN_NAME}:dev_${COMMIT}

PWD=$(shell pwd)

.PHONY: gen docker gen_mock

all: init gen run 

init:
	go install github.com/swaggo/swag/cmd/swag@latest
	go install github.com/golang/mock/mockgen@v1.6.0
	go mod tidy 

gen:
	$(HOME)/go/bin/swag init -g server/main.go 

run: gen
	go run main.go -f ./samples/yaml/timeplus.yaml

build:
	go build 

run_materialize_client:
	docker run -it --rm jbergknoff/postgresql-client postgresql://materialize@host.docker.internal:6875/materialize

kafka_consume:
	docker exec -it kafka /usr/bin/kafka-console-consumer \
		--topic order --bootstrap-server localhost:9092
ksql_client:
	docker exec -it ksqldb-cli ksql http://ksqldb-server:8088

docker: Dockerfile
	docker build -t $(IMAGE_NAME) .

docker_run:
	docker run -it -v $(PWD)/samples:/samples $(IMAGE_NAME) -f /samples/yaml/timeplus.yaml

push:
	docker tag $(IMAGE_NAME) $(FULLNAME)
	docker tag $(IMAGE_NAME) $(DOCKER_ID_USER)/${BIN_NAME}:latest
	docker push $(FULLNAME)
	docker push $(DOCKER_ID_USER)/${BIN_NAME}:latest