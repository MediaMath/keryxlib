#/bin/bash

set -eu

. "$(dirname $0)/common.bash"

setup
start
trap finish EXIT

SQL='insert into test (count) values (1), (2), (3), (4), (5), (6);'
INVERT="--invert"
FILTER='"smoke_test.public.test":["*"]'

CONDITION='
{
	"has_message": {"rel": "test"}
}'

run_test

echo "TEST PASSED"

exit
