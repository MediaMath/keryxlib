.PHONY:	test golint

test: golint
	golint ./...
	go vet ./...
	go test $(TEST_VERBOSITY) ./...

golint:
	go get github.com/golang/lint/golint

