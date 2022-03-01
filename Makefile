#!/bin/bash 

# Parameters for Go
export BINARY_NAME=pg-backup

export GOPATH=$(HOME)/go

all: | clean build

build:
	go mod tidy
	go build -o bin/$(BINARY_NAME)  ./main.go
	cp sample.json ./bin

clean:
	rm -f bin/$(BINARY_NAME)
