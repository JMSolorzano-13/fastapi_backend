#!/usr/bin/env bash
# Send each line of a JSONL file to Service Bus. Line format:
#   {"queue":"data-queue-download-zips-s3","body":{...}}
#
# Usage:
#   export SB_NAMESPACE_HOST="sb-….servicebus.windows.net"
#   ./scripts/sat/az_ws_enqueue_jsonl.sh path/to/file.jsonl
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT" || exit 1

: "${SB_NAMESPACE_HOST:?Set SB_NAMESPACE_HOST}"

JSONL="${1:?path to .jsonl}"
if [[ ! -f "${JSONL}" ]]; then
  echo "ERROR: not found: ${JSONL}" >&2
  exit 1
fi

ENQ="${ROOT}/scripts/sat/az_ws_enqueue_sb_json.sh"
chmod +x "${ENQ}" 2>/dev/null || true

line_no=0
while IFS= read -r line || [[ -n "${line}" ]]; do
  [[ -z "${line// }" ]] && continue
  ((line_no++)) || true
  q="$(python3 -c "import json,sys; print(json.loads(sys.argv[1])['queue'])" "${line}")"
  tmp="$(mktemp)"
  python3 -c "import json,sys; print(json.dumps(json.loads(sys.argv[1])['body'], ensure_ascii=False))" "${line}" >"${tmp}"
  SB_QUEUE="${q}" SB_BODY_FILE="${tmp}" "${ENQ}" || { rm -f "${tmp}"; exit 1; }
  rm -f "${tmp}"
  echo "  [${line_no}] -> ${q}" >&2
done <"${JSONL}"

echo "Done: ${line_no} message(s)." >&2
