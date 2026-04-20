#!/usr/bin/env bash
# Enqueue many verify messages: one JSON object per line (NDJSON) in VERIFY_NDJSON_FILE.
# Same AAD + Service Bus contract as az_ws_enqueue_verify.sh — no database connection.
#
# Build VERIFY_NDJSON_FILE however you prefer (export, script, API). Postgres is optional and
# only for generating lines, not for publishing.
#
# Usage:
#   export SB_NAMESPACE_HOST="sb-siigofiscal-dev-fbd2191d.servicebus.windows.net"
#   export VERIFY_NDJSON_FILE="/path/to/verify_payloads.ndjson"
#   ./scripts/sat/az_ws_enqueue_verify_batch.sh
#
# Optional: SLEEP_SECS=0.2 between posts to avoid throttling

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT" || exit 1

: "${SB_NAMESPACE_HOST:?}"
: "${VERIFY_NDJSON_FILE:?}"
: "${SB_QUEUE_VERIFY:=data-queue-verify-request}"
: "${SLEEP_SECS:=0.2}"

if [[ ! -f "${VERIFY_NDJSON_FILE}" ]]; then
  echo "ERROR: VERIFY_NDJSON_FILE not found: ${VERIFY_NDJSON_FILE}" >&2
  exit 1
fi

TOKEN="$(az account get-access-token --resource https://servicebus.azure.net --query accessToken -o tsv)"
N=0
OK=0
FAIL=0

while IFS= read -r line || [[ -n "${line}" ]]; do
  [[ -z "${line//[[:space:]]/}" ]] && continue
  N=$((N + 1))
  RESP="$(mktemp)"
  HTTP_CODE="$(curl -sS -o "$RESP" -w "%{http_code}" -X POST \
    "https://${SB_NAMESPACE_HOST}/${SB_QUEUE_VERIFY}/messages" \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "Content-Type: application/json; charset=utf-8" \
    -d "${line}")"
  if [[ "${HTTP_CODE}" == "201" ]]; then
    OK=$((OK + 1))
    echo "OK line ${N}" >&2
  else
    FAIL=$((FAIL + 1))
    echo "FAIL line ${N} HTTP ${HTTP_CODE}" >&2
    cat "$RESP" >&2
  fi
  rm -f "$RESP"
  sleep "${SLEEP_SECS}"
done < "${VERIFY_NDJSON_FILE}"

echo "Done: total=${N} ok=${OK} fail=${FAIL}" >&2
[[ "${FAIL}" -eq 0 ]]
