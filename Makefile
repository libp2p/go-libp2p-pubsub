CURRENT_DIRECTORY := $(shell pwd)
TESTS_TO_RUN := $(shell go list ./... | grep -v /integrationTests/ | grep -v mock)

test: clean-test
	go test -timeout 20m ./...

clean-test:
	go clean -testcache ./...