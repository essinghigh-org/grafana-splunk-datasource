.PHONY: all build build-splunk-datasource

SHELL = /bin/sh

all: build

build: build-splunk-datasource

build-splunk-datasource:
	yarn install --immutable
	yarn build
