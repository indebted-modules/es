#!/bin/bash
set -e -o pipefail

exec 5>&1
output="$(gofmt -l "$@" | tee /dev/fd/5)"
[[ -z "$output" ]]
