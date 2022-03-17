BIN_NAME ?= dataloader
VERSION ?= dev
IMAGE_NAME ?= $(BIN_NAME):$(VERSION)
DOCKER_ID_USER ?= timeplus

DATE=$(shell gdate -Iseconds)
COMMIT=$(shell git rev-parse --short HEAD)
FULLNAME=$(DOCKER_ID_USER)/${BIN_NAME}:dev_${COMMIT}

.PHONY: docker

build:
	go build cmd/dataloader.go  

run:
	go run cmd/dataloader.go --config=./config/config.yml

docker: Dockerfile
	docker build -t $(IMAGE_NAME) .

push:
	docker tag $(IMAGE_NAME) $(FULLNAME)
	docker push $(FULLNAME)

