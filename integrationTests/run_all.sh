#/bin/bash

set -eu

SCRIPT_PATH=`dirname $0`

"$SCRIPT_PATH/test_end_to_end.sh"
"$SCRIPT_PATH/test_end_to_end_with_other.sh"
"$SCRIPT_PATH/test_filter.sh"
