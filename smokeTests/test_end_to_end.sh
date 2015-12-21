#/bin/bash

set -eu

. "$(dirname $0)/common.bash"

setup
start
trap finish EXIT

SQL='insert into test (count) values (1), (2), (3), (4), (5), (6);'

CONDITION='
{
	"all_of": [
		{ "has_message": {
			"type": 2,
			"db": "smoke_test",
			"ns": "public",
			"rel": "test",
			"fields_match": {"count": "1"}}
		},
		{ "has_message": {
			"type": 2,
			"db": "smoke_test",
			"ns": "public",
			"rel": "test",
			"fields_match": {"count": "2"}}
		},
		{ "has_message": {
			"type": 2,
			"db": "smoke_test",
			"ns": "public",
			"rel": "test",
			"fields_match": {"count": "3"}}
		},
		{ "has_message": {
			"type": 2,
			"db": "smoke_test",
			"ns": "public",
			"rel": "test",
			"fields_match": {"count": "4"}}
		},

		{ "has_message": {
			"type": 2,
			"db": "smoke_test",
			"ns": "public",
			"rel": "test",
			"fields_match": {"count": "5"}}
		},
		{ "has_message": {
			"type": 2,
			"db": "smoke_test",
			"ns": "public",
			"rel": "test",
			"fields_match": {"count": "6"}}
		}
	]
}'

run_test

echo "TEST PASSED"

exit
