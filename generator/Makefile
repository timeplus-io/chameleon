
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
	go run main.go -f ./samples/splunk_ob.json

run_neutron:
	go run main.go -f ./samples/neutron.json

run_neutron_ob:
	go run main.go -f ./samples/neutron_ob_throughput.json

run_splunk:
	go run main.go -f ./samples/splunk.json

run_splunk_ob:
	go run main.go -f ./samples/splunk_ob_throughput.json

run_materialize:
	go run main.go -f ./samples/materialize.json

run_materialize_client:
	docker run -it --rm jbergknoff/postgresql-client postgresql://materialize@host.docker.internal:6875/materialize

docker: Dockerfile
	docker build -t $(IMAGE_NAME) .

docker_run:
	docker run -p 3000:3000 $(IMAGE_NAME)

push:
	docker tag $(IMAGE_NAME) $(FULLNAME)
	docker push $(FULLNAME)