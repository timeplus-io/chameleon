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

docker: Dockerfile
	docker build -t $(IMAGE_NAME) .

docker_run:
	docker run -p 8000:8000 $(IMAGE_NAME) --proton-addr host.docker.internal

push:
	docker tag $(IMAGE_NAME) $(FULLNAME)
	docker push $(FULLNAME)

