#!/bin/bash
set -e
(
  if [[ $# -eq 0 ]]; then
    &>2 echo "a path to an executable must be passed"
    exit 1
  fi
  export DYNQ=$1
  cd "$(dirname "$0")/.."
  e2e-tests/node_modules/.bin/vitest --config e2e-tests/vitest.config.ts --run
)