#!/usr/bin/env bash
# Enqueue ONE SAT verify message to Azure Service Bus (same JSON body the worker expects:
# chalicelib Query model → process_sqs_verify_query).
#
# This path does NOT use Postgres: you supply the JSON (from a prior export, API, or template).
# Monitoring: az servicebus queue show … / az containerapp logs … worker
#
# Requires: Azure CLI (`az`), `curl`, `az login`, RBAC **Azure Service Bus Data Sender** on the namespace.
# Token: `az account get-access-token --resource https://servicebus.azure.net`
#
# Usage:
#   export SB_NAMESPACE_HOST="sb-siigofiscal-dev-fbd2191d.servicebus.windows.net"
#   export SB_QUEUE_VERIFY="data-queue-verify-request"   # optional, default below
#   export VERIFY_BODY_FILE="/path/to/query.json"
#   ./scripts/sat/az_ws_enqueue_verify.sh
#
# Example minimal query.json (adjust UUIDs, wid, cid, name = SAT solicitud id, dates):
#   {
#     "company_identifier": "0f4d7bb3-0c1a-4a4c-abac-7d6f5a8404bf",
#     "identifier": "<internal sat_query uuid>",
#     "download_type": "ISSUED",
#     "request_type": "CFDI",
#     "state": "SENT",
#     "name": "<SAT package request id>",
#     "start": "2024-01-01T00:00:00",
#     "end": "2024-03-31T00:00:00",
#     "is_manual": false,
#     "packages": [],
#     "cfdis_qty": null,
#     "wid": 14,
#     "cid": 26,
#     "sent_date": "2026-04-19T12:00:00",
#     "origin_sent_date": "2026-04-19T12:00:00",
#     "technology": "WebService",
#     "origin_identifier": null,
#     "ws_verify_retries": 0
#   }

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT" || exit 1

: "${SB_NAMESPACE_HOST:?Set SB_NAMESPACE_HOST (e.g. sb-….servicebus.windows.net)}"
: "${VERIFY_BODY_FILE:?Path to JSON file (Query wire format)}"
: "${SB_QUEUE_VERIFY:=data-queue-verify-request}"

if ! command -v curl >/dev/null 2>&1; then
  echo "ERROR: curl is required." >&2
  exit 1
fi

if [[ ! -f "${VERIFY_BODY_FILE}" ]]; then
  echo "ERROR: VERIFY_BODY_FILE not found: ${VERIFY_BODY_FILE}" >&2
  exit 1
fi

BODY="$(cat "${VERIFY_BODY_FILE}")"
RESP="$(mktemp)"
trap 'rm -f "$RESP"' EXIT

echo "POST https://${SB_NAMESPACE_HOST}/${SB_QUEUE_VERIFY}/messages (AAD token via az)" >&2

TOKEN="$(az account get-access-token --resource https://servicebus.azure.net --query accessToken -o tsv)"
HTTP_CODE="$(curl -sS -o "$RESP" -w "%{http_code}" -X POST \
  "https://${SB_NAMESPACE_HOST}/${SB_QUEUE_VERIFY}/messages" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json; charset=utf-8" \
  -d "${BODY}")"

if [[ "${HTTP_CODE}" != "201" ]]; then
  echo "ERROR: Service Bus returned HTTP ${HTTP_CODE}" >&2
  cat "$RESP" >&2
  exit 1
fi

echo "OK: verify message accepted (HTTP 201)." >&2
