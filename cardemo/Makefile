BIN_NAME ?= cardemo
VERSION ?= dev
IMAGE_NAME ?= $(BIN_NAME):$(VERSION)
DOCKER_ID_USER ?= timeplus

DATE=$(shell gdate -Iseconds)
COMMIT=$(shell git rev-parse --short HEAD)
FULLNAME=$(DOCKER_ID_USER)/${BIN_NAME}:dev_${COMMIT}

.PHONY: gen docker gen_mock

build:
	go build 

run:
	go run main.go --config ./.cardemo.yaml -f ./sinks.yaml

docker: Dockerfile
	docker build -t $(IMAGE_NAME) .

docker_run:
	docker run $(IMAGE_NAME)

push:
	docker tag $(IMAGE_NAME) $(FULLNAME)
	docker tag $(IMAGE_NAME) $(DOCKER_ID_USER)/${BIN_NAME}:latest
	docker push $(FULLNAME)
	docker push $(DOCKER_ID_USER)/${BIN_NAME}:latest