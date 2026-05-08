#!/usr/bin/env bash
set -euo pipefail

JOB="${1:-}"

if [ -z "$JOB" ]; then
    echo "Usage: ./bin/run_job.sh <job>"
    exit 1
fi

ROOT="/opt/metrigo"
LOCK_DIR="${ROOT}/locks"
LOG_DIR="${ROOT}/logs"

mkdir -p "${LOCK_DIR}"
mkdir -p "${LOG_DIR}"

LOCK_FILE="${LOCK_DIR}/${JOB}.lock"
LOG_FILE="${LOG_DIR}/${JOB}.log"

exec 9>"${LOCK_FILE}"

if ! flock -n 9; then
    echo "$(date '+%F %T') | skip ${JOB} (already running)" >> "${LOG_FILE}"
    exit 0
fi

echo "" >> "${LOG_FILE}"
echo "==================================================" >> "${LOG_FILE}"
echo "$(date '+%F %T') | start ${JOB}" >> "${LOG_FILE}"

cd "${ROOT}"

docker compose run --rm worker python main.py "${JOB}" >> "${LOG_FILE}" 2>&1

echo "$(date '+%F %T') | finish ${JOB}" >> "${LOG_FILE}"