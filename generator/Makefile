
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
	$(HOME)/go/bin/swag init -g internal/server/main.go 

run: gen
	go run main.go -S

build:
	go build 

docker: Dockerfile
	docker build -t $(IMAGE_NAME) .

docker_run:
	docker run -it -v $(PWD)/samples:/samples $(IMAGE_NAME) -f /samples/yaml/proton.yaml

push:
	docker tag $(IMAGE_NAME) $(FULLNAME)
	docker tag $(IMAGE_NAME) $(DOCKER_ID_USER)/${BIN_NAME}:latest
	docker push $(FULLNAME)
	docker push $(DOCKER_ID_USER)/${BIN_NAME}:latest
