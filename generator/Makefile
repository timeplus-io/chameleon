
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
	go mod tidy

gen:
	$(HOME)/go/bin/swag init -g server/main.go 

run:
	go run main.go