#!/usr/bin/env bash
set -euo pipefail

cd /opt/metrigo

docker compose exec -T api python -m app.jobs.build_agg_sku_snapshot \
  --seller-id main \
  --tax-rate 0.06