#!/usr/bin/env bash
# Linux/macOS: tests (blocking) -> scales -> report.   Usage: scripts/run_all.sh [smoke baseline extension]
set -euo pipefail
cd "$(dirname "$0")/.."
SCALES=("${@:-smoke baseline extension}"); [ $# -eq 0 ] && SCALES=(smoke baseline extension)
export PYTHONPATH=src TZ=UTC SPARK_LOCAL_HOSTNAME=localhost
: "${JAVA_HOME:?Set JAVA_HOME to a JDK 17+}"
PY=.venv/bin/python
$PY -m pytest -p no:cacheprovider -rs
for s in "${SCALES[@]}"; do $PY -m bench.cli run --scale "$s"; done
$PY -m bench.cli report
