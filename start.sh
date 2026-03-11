#!/usr/bin/env bash
set -euo pipefail

HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-8000}"
WORKERS="${WORKERS:-4}"

exec gunicorn serve:app \
  --worker-class uvicorn.workers.UvicornWorker \
  --workers "$WORKERS" \
  --bind "${HOST}:${PORT}" \
  --timeout 120 \
  --access-logfile - \
  --error-logfile -
