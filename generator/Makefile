
BIN_NAME ?= generator
VERSION ?= dev
IMAGE_NAME ?= $(BIN_NAME):$(VERSION)
DOCKER_ID_USER ?= timeplus

DATE=$(shell gdate -Iseconds)
COMMIT=$(shell git rev-parse --short HEAD)
FULLNAME=$(DOCKER_ID_USER)/${BIN_NAME}:dev_${COMMIT}

.PHONY: gen docker gen_mock

all: init gen run 

init:
	go install github.com/swaggo/swag/cmd/swag@latest
	go install github.com/golang/mock/mockgen@v1.6.0
	go mod tidy -go=1.16 && go mod tidy -go=1.17

gen:
	$(HOME)/go/bin/swag init -g server/main.go 

run: gen
	go run main.go -f ./samples/source_ob.json

docker: Dockerfile
	docker build -t $(IMAGE_NAME) .

docker_run:
	docker run -p 3000:3000 $(IMAGE_NAME)

push:
	docker tag $(IMAGE_NAME) $(FULLNAME)
	docker push $(FULLNAME)