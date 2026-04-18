#!/usr/bin/env python3
"""Emit FastAPI HTTP routes from OpenAPI (under ``/api``) as JSON for Chalice parity checks.

Usage:
  LOCAL_INFRA=0 poetry run python scripts/dump_fastapi_http_routes.py
  LOCAL_INFRA=1 poetry run python scripts/dump_fastapi_http_routes.py

Output: one JSON object to stdout: {"routes": [["/api/Path", "POST"], ...]}
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

_FASTAPI_ROOT = Path(__file__).resolve().parents[1]
if str(_FASTAPI_ROOT) not in sys.path:
    sys.path.insert(0, str(_FASTAPI_ROOT))


def main() -> None:
    if "LOCAL_INFRA" not in os.environ and len(sys.argv) > 1:
        os.environ["LOCAL_INFRA"] = sys.argv[1]
    os.environ.setdefault("LOCAL_INFRA", "0")

    from dotenv import load_dotenv

    load_dotenv()

    from fastapi.openapi.utils import get_openapi

    from main import app

    schema = get_openapi(title=app.title, version=app.version, routes=app.routes)
    skip = {"OPTIONS", "HEAD"}
    pairs: list[list[str]] = []
    for path in sorted(schema.get("paths", {})):
        if not path.startswith("/api"):
            continue
        for method in sorted(schema["paths"][path].keys()):
            m = method.upper()
            if m in skip:
                continue
            pairs.append([path, m])

    pairs.sort(key=lambda x: (x[0], x[1]))
    json.dump({"routes": pairs}, sys.stdout)


if __name__ == "__main__":
    main()
