#!/usr/bin/env bash
# POST one JSON body to an Azure Service Bus queue (HTTP + AAD), same pattern as az_ws_enqueue_verify.sh.
#
# Requires: az login, RBAC Azure Service Bus Data Sender on the namespace.
#
# Usage:
#   export SB_NAMESPACE_HOST="sb-siigofiscal-dev-fbd2191d.servicebus.windows.net"
#   export SB_QUEUE="queue-process-xml-query"
#   export SB_BODY_FILE="/tmp/payload.json"
#   ./scripts/sat/az_ws_enqueue_sb_json.sh
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT" || exit 1

: "${SB_NAMESPACE_HOST:?Set SB_NAMESPACE_HOST (e.g. sb-….servicebus.windows.net)}"
: "${SB_QUEUE:?Set SB_QUEUE (hyphen name, e.g. data-queue-download-zips-s3)}"
: "${SB_BODY_FILE:?Path to JSON body}"

if ! command -v curl >/dev/null 2>&1; then
  echo "ERROR: curl is required." >&2
  exit 1
fi
if [[ ! -f "${SB_BODY_FILE}" ]]; then
  echo "ERROR: SB_BODY_FILE not found: ${SB_BODY_FILE}" >&2
  exit 1
fi

BODY="$(cat "${SB_BODY_FILE}")"
RESP="$(mktemp)"
trap 'rm -f "$RESP"' EXIT

TOKEN="$(az account get-access-token --resource https://servicebus.azure.net --query accessToken -o tsv)"
HTTP_CODE="$(curl -sS -o "$RESP" -w "%{http_code}" -X POST \
  "https://${SB_NAMESPACE_HOST}/${SB_QUEUE}/messages" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json; charset=utf-8" \
  -d "${BODY}")"

if [[ "${HTTP_CODE}" != "201" ]]; then
  echo "ERROR: queue=${SB_QUEUE} HTTP ${HTTP_CODE}" >&2
  cat "$RESP" >&2
  exit 1
fi
echo "OK: ${SB_QUEUE} (HTTP 201)" >&2
