"""Prefer fastapi_backend `chalicelib` over monorepo `backend/` for this package."""

from __future__ import annotations

import os
import sys

# Collection imports ``chalicelib`` → ``envars``; parent shells may export AUTH_BACKEND=local_jwt.
if os.environ.get("AUTH_BACKEND", "").strip().lower() == "local_jwt":
    os.environ.setdefault(
        "JWT_SECRET",
        "pytest-default-jwt-secret-key-32chars-minimum-xx",
    )
from pathlib import Path

_fastapi_backend = Path(__file__).resolve().parents[2]
_backend = _fastapi_backend.parent / "backend"
if _backend.is_dir():
    _backend_resolved = str(_backend.resolve())
    sys.path[:] = [p for p in sys.path if Path(p).resolve() != Path(_backend_resolved)]
if str(_fastapi_backend) not in sys.path:
    sys.path.insert(0, str(_fastapi_backend))
for _k in list(sys.modules):
    if _k == "chalicelib" or _k.startswith("chalicelib."):
        del sys.modules[_k]
