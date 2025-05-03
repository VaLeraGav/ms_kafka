PROJECT_NAME_P = ms_kafka_producer
PROJECT_NAME_C = ms_kafka_consumer

## help: print this help message
.PHONY: help
help:
	@echo 'Usage:'
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' |  sed -e 's/^/ /'

## init-p: used to initialize the Go project, tidy, docker, migration, build and deploy
.PHONY: init-p
init-p:
	go mod tidy
	docker compose up -d
	CGO_ENABLED=1 go build -o build/package/$(PROJECT_NAME_P) cmd/$(PROJECT_NAME_P)/main.go

## fast-start-p: quick launch of ms_kafka_producer
.PHONY: fast-start-p
fast-start-p:
	go run cmd/$(PROJECT_NAME_P)/main.go

## start-p: build start of ms_kafka_producer
.PHONY: start-p
start-p:
	CGO_ENABLED=1 go build -o build/package/$(PROJECT_NAME_P) cmd/$(PROJECT_NAME_P)/main.go
	build/package/$(PROJECT_NAME_P)

## fast-start-c: quick launch of ms_kafka_consumer
.PHONY: fast-start-c
fast-start-c:
	go run cmd/$(PROJECT_NAME_C)/main.go

## start-c: build start of ms_kafka_consumer
.PHONY: start-c
start-c:
	CGO_ENABLED=1 go build -o build/package/$(PROJECT_NAME_C) cmd/$(PROJECT_NAME_C)/main.go
	build/package/$(PROJECT_NAME_C)