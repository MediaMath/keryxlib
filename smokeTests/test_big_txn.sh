#/bin/bash

set -eu

. "$(dirname $0)/common.bash"

setup
start
trap finish EXIT

MAX_MSG=3

SQL='insert into test (count) values (1), (2), (3), (4), (5), (6);'

CONDITION='
{
	"all_of": [{ "has_message": 
			{
				"db": "smoke_test",
				"ns": "public",
				"rel": "test"
			}
		},
		{ "transaction_is": { "big":true}}]
}'

run_test

echo "TEST PASSED"

exit
