#!/bin/bash
(cd "$(dirname "$0")/.." && e2e-tests/node_modules/.bin/vitest --config e2e-tests/vitest.config.ts  --run)