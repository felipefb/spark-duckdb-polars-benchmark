#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python3.12 -m venv .venv || python3 -m venv .venv
.venv/bin/pip install --upgrade pip
# requirements.lock was frozen on Windows; on Linux/macOS install the pinned top-level set:
.venv/bin/pip install -r requirements.txt
