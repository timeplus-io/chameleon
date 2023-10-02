BIN_NAME ?= cardemo
VERSION ?= dev
IMAGE_NAME ?= $(BIN_NAME):$(VERSION)
DOCKER_ID_USER ?= timeplus

DATE=$(shell gdate -Iseconds)
COMMIT=$(shell git rev-parse --short HEAD)
FULLNAME=$(DOCKER_ID_USER)/${BIN_NAME}:dev_${COMMIT}
PWD=$(shell pwd)

.PHONY: gen docker gen_mock

build:
	go build 

run:
	go run main.go --config ./.cardemo.yaml -f ./sinks.yaml

run_proton:
	docker run -d --pull always -p 3218:3218 -p 8463:8463 --name proton ghcr.io/timeplus-io/proton:latest

docker: Dockerfile
	docker build -t $(IMAGE_NAME) .

docker_run:
	docker run \
		-v $(PWD)/routes.json:/timeplus/routes.json:ro \
		-v $(PWD)/.cardemo.yaml:/timeplus/.cardemo.yaml:ro \
		-v $(PWD)/sinks.yaml:/timeplus/sinks.yaml:ro \
		$(IMAGE_NAME) --config /timeplus/.cardemo.yaml -f ./sinks.yaml

push:
	docker tag $(IMAGE_NAME) $(FULLNAME)
	docker tag $(IMAGE_NAME) $(DOCKER_ID_USER)/${BIN_NAME}:latest
	docker push $(FULLNAME)
	docker push $(DOCKER_ID_USER)/${BIN_NAME}:latest