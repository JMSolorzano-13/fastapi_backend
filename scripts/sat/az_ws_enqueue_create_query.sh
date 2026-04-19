#!/usr/bin/env bash
# Enqueue one SAT WebService "create query" message to Azure Service Bus (same JSON as
# scripts.sat.generate_sat_requests / FastAPI bus → CreateQuery).
#
# Requires: Azure CLI (`az`), `curl`, `jq`, `az login`, and RBAC **Azure Service Bus Data Sender**
# on the namespace (or SAS — not implemented here). Token: `az account get-access-token`
# for resource `https://servicebus.azure.net`.
#
# Note: `az rest` to the Service Bus **data plane** often returns **401** even with Data Sender;
# this script uses **curl** + the same AAD token (still “Azure CLI–driven” operator flow).
#
# Usage (example siigofiscal dev — adjust SB_NAMESPACE_HOST / RG for your env):
#   export SB_NAMESPACE_HOST="sb-siigofiscal-dev-fbd2191d.servicebus.windows.net"
#   export SB_QUEUE_CREATE="queue-create-query"
#   export COMPANY_IDENTIFIER="0f4d7bb3-0c1a-4a4c-abac-7d6f5a8404bf"
#   export COMPANY_RFC="SIE200729UA0"
#   export WID="14"
#   export CID="26"
#   export REQUEST_TYPE="CFDI"          # CFDI | METADATA
#   export DOWNLOAD_TYPE="ISSUED"       # ISSUED | RECEIVED
#   export START_ISO="2024-01-01T00:00:00"
#   export END_ISO="2024-03-31T00:00:00"
#   ./scripts/sat/az_ws_enqueue_create_query.sh
#
# After enqueue, inspect queue depth (example RG/namespace from Terraform):
#   az servicebus queue show -g rg-siigofiscal-dev --namespace-name sb-siigofiscal-dev-fbd2191d \
#     --name queue-create-query --query "countDetails" -o jsonc

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT" || exit 1

: "${SB_NAMESPACE_HOST:?Set SB_NAMESPACE_HOST (e.g. sb-….servicebus.windows.net)}"
: "${SB_QUEUE_CREATE:=queue-create-query}"
: "${COMPANY_IDENTIFIER:?}"
: "${COMPANY_RFC:?}"
: "${WID:?}"
: "${CID:?}"
: "${REQUEST_TYPE:?}"
: "${DOWNLOAD_TYPE:?}"
: "${START_ISO:?}"
: "${END_ISO:?}"

if ! command -v jq >/dev/null 2>&1; then
  echo "ERROR: jq is required." >&2
  exit 1
fi
if ! command -v curl >/dev/null 2>&1; then
  echo "ERROR: curl is required." >&2
  exit 1
fi

BODY="$(
  jq -nc \
    --arg ci "$COMPANY_IDENTIFIER" \
    --arg cr "$COMPANY_RFC" \
    --arg dt "$DOWNLOAD_TYPE" \
    --arg rt "$REQUEST_TYPE" \
    --argjson wid "$WID" \
    --argjson cid "$CID" \
    --arg st "$START_ISO" \
    --arg en "$END_ISO" \
    '{
      company_identifier: $ci,
      company_rfc: $cr,
      download_type: $dt,
      request_type: $rt,
      is_manual: true,
      start: $st,
      end: $en,
      query_origin: null,
      origin_sent_date: null,
      wid: $wid,
      cid: $cid
    }'
)"

RESP="$(mktemp)"
trap 'rm -f "$RESP"' EXIT

echo "POST https://${SB_NAMESPACE_HOST}/${SB_QUEUE_CREATE}/messages (AAD token via az)" >&2

TOKEN="$(az account get-access-token --resource https://servicebus.azure.net --query accessToken -o tsv)"
HTTP_CODE="$(curl -sS -o "$RESP" -w "%{http_code}" -X POST \
  "https://${SB_NAMESPACE_HOST}/${SB_QUEUE_CREATE}/messages" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json; charset=utf-8" \
  -d "${BODY}")"

if [[ "${HTTP_CODE}" != "201" ]]; then
  echo "ERROR: Service Bus returned HTTP ${HTTP_CODE}" >&2
  cat "$RESP" >&2
  exit 1
fi

echo "OK: message accepted (HTTP 201)." >&2
