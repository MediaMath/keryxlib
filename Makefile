.PHONY:	test golint smoke-test
	
# Copyright 2015 MediaMath <http://www.mediamath.com>.  All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

test: golint
	golint ./...
	go vet ./...
	go test $(TEST_VERBOSITY) ./...

golint:
	go get github.com/golang/lint/golint

smoke-test:
	go install  github.com/MediaMath/keryxlib/smoke
	/bin/bash ./integrationTests/run_all.sh
