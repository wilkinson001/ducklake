#!/bin/bash
set -euo pipefail
for f in /scripts/*.sql; do
  envsubst <"$f" | duckdb
done
