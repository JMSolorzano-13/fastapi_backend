#!/usr/bin/env bash
# Read-only Service Bus queue depths for SAT pipeline (az cli only — no DB).
#
# Usage:
#   export SB_RG="rg-siigofiscal-dev"
#   export SB_NAMESPACE="sb-siigofiscal-dev-fbd2191d"
#   ./scripts/sat/az_ws_sat_queues_status.sh

set -euo pipefail

: "${SB_RG:?Set SB_RG (resource group)}"
: "${SB_NAMESPACE:?Set SB_NAMESPACE (Service Bus namespace name)}"

QUEUES=(
  "queue-create-query"
  "queue-create-metadata-query"
  "data-queue-verify-request"
  "data-queue-download-zips-s3"
  "data-queue-metadata"
  "queue-process-xml-query"
  "queue-complete-cfdi"
  "queue-updater-query"
)

for q in "${QUEUES[@]}"; do
  echo "=== ${q} ==="
  az servicebus queue show -g "${SB_RG}" --namespace-name "${SB_NAMESPACE}" --name "${q}" \
    --query "countDetails" -o jsonc 2>/dev/null || echo "(missing or no access)"
done
