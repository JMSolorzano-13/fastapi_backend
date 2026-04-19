"""Session debug NDJSON (Cursor debug mode). Do not log secrets or PII."""

from __future__ import annotations

import json
import time
from typing import Any

_LOG_PATHS = (
    "/Users/juanmanuelsolorzano/Developer/ez/local_siigo_fiscal/.cursor/debug-bb286d.log",
    "/tmp/debug-bb286d.log",
)
_SESSION = "bb286d"


# region agent log
def agent_debug_log(
    location: str,
    message: str,
    hypothesis_id: str,
    data: dict[str, Any] | None = None,
) -> None:
    payload = {
        "sessionId": _SESSION,
        "hypothesisId": hypothesis_id,
        "location": location,
        "message": message,
        "data": data or {},
        "timestamp": int(time.time() * 1000),
    }
    line = json.dumps(payload, default=str) + "\n"
    for path in _LOG_PATHS:
        try:
            with open(path, "a", encoding="utf-8") as f:
                f.write(line)
            break
        except OSError:
            continue
    print("[AGENT_DEBUG]", line.strip(), flush=True)


# endregion
