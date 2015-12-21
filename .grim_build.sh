#!/bin/bash

set -eu

. /opt/golang/go1.5.1/bin/go_env.sh

export GOPATH="$(pwd)/go"
export PATH="/usr/lib/postgresql/9.1/bin:$GOPATH/bin:$PATH"

cd "./$CLONE_PATH"

go get ./...

make test smoke-test


