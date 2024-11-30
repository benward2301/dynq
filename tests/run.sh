#!/bin/bash
set -e

if [[ $# -eq 0 ]]; then
  >&2 echo "a path to an executable must be passed"
  exit 1
fi

(
  cd "$(dirname "$0")"
  npm i
)

DYNQ=$1

if [[ -v 2 ]]; then
  DYNQ="$2 $DYNQ"
fi

export DYNQ
export FORCE_COLOR=1

(
  cd "$(dirname "$0")/.."
  tests/node_modules/.bin/vitest --config tests/vitest.config.ts --run
)