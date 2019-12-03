#!/bin/bash
set -e -o pipefail

failed=false

for file in "$@"; do
	if ! golint -set_exit_status "$file" 2>&1
	then
		failed=true
	fi
done

if [[ $failed == "true" ]]; then
	exit 1
fi
