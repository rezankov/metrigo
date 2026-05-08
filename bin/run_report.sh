#!/usr/bin/env bash
set -euo pipefail

REPORT="${1:-}"

if [ -z "$REPORT" ]; then
    echo "Usage: ./bin/run_report.sh <report>"
    exit 1
fi

cd /opt/metrigo

if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

docker compose run --rm worker python "reports/${REPORT}.py"