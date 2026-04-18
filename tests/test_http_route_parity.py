"""Chalice (blueprints) vs FastAPI OpenAPI — same (path, method) pairs under ``/api``."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_BACKEND_DIR = _REPO_ROOT / "backend"
_FASTAPI_DIR = _REPO_ROOT / "fastapi_backend"
_CHALICE_DUMP = _BACKEND_DIR / "scripts" / "dump_chalice_http_routes.py"
_FASTAPI_DUMP = _FASTAPI_DIR / "scripts" / "dump_fastapi_http_routes.py"


def _require_layout() -> None:
    if not _CHALICE_DUMP.is_file():
        pytest.skip(f"Missing Chalice dump script: {_CHALICE_DUMP}")
    if not _FASTAPI_DUMP.is_file():
        pytest.skip(f"Missing FastAPI dump script: {_FASTAPI_DUMP}")
    if shutil.which("poetry") is None:
        pytest.skip("poetry not on PATH")


def _run_dump(*, project_dir: Path, script_path: Path, local_infra: str) -> list[list[str]]:
    # Drop inherited venv hints so `poetry run` in another project resolves its own env
    # (e.g. pytest under fastapi_backend must not pass VIRTUAL_ENV into backend subprocess).
    env = os.environ.copy()
    for key in ("VIRTUAL_ENV", "POETRY_ACTIVE", "PYTHONHOME"):
        env.pop(key, None)
    env.pop("PYTHONPATH", None)
    env["LOCAL_INFRA"] = local_infra
    rel = script_path.relative_to(project_dir)
    proc = subprocess.run(
        ["poetry", "run", "python", str(rel)],
        cwd=str(project_dir),
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    if proc.returncode != 0:
        pytest.fail(
            f"dump failed (cwd={project_dir}, LOCAL_INFRA={local_infra}):\n"
            f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"
        )
    data = json.loads(proc.stdout)
    return data["routes"]


def _normalize_path(path: str) -> str:
    """OpenAPI often includes a trailing slash for ``@router.*("/")`` mounts; Chalice does not."""
    return path.rstrip("/") if path.endswith("/") else path


def _canonical_routes(pairs: list[list[str]]) -> list[list[str]]:
    seen = {(_normalize_path(p), m.upper()) for p, m in pairs}
    return sorted([list(t) for t in seen])


def _diff_lines(
    left: list[list[str]], right: list[list[str]], label_left: str, label_right: str
) -> str:
    s_left = {tuple(p) for p in left}
    s_right = {tuple(p) for p in right}
    only_l = sorted(s_left - s_right)
    only_r = sorted(s_right - s_left)
    parts = [
        f"{label_left} count={len(left)} {label_right} count={len(right)}",
        f"only in {label_left}: {len(only_l)}",
    ]
    parts.extend(f"  {p}" for p in only_l[:50])
    if len(only_l) > 50:
        parts.append(f"  ... and {len(only_l) - 50} more")
    parts.append(f"only in {label_right}: {len(only_r)}")
    parts.extend(f"  {p}" for p in only_r[:50])
    if len(only_r) > 50:
        parts.append(f"  ... and {len(only_r) - 50} more")
    return "\n".join(parts)


def test_chalice_routes_match_fastapi_openapi() -> None:
    """Runs ``LOCAL_INFRA=0`` and ``1`` in one test to avoid xdist parallel ``poetry`` storms."""
    _require_layout()
    for local_infra in ("0", "1"):
        chalice_routes = _run_dump(
            project_dir=_BACKEND_DIR,
            script_path=_CHALICE_DUMP,
            local_infra=local_infra,
        )
        fastapi_routes = _run_dump(
            project_dir=_FASTAPI_DIR,
            script_path=_FASTAPI_DUMP,
            local_infra=local_infra,
        )
        ch_canon = _canonical_routes(chalice_routes)
        fa_canon = _canonical_routes(fastapi_routes)
        if ch_canon != fa_canon:
            pytest.fail(
                f"LOCAL_INFRA={local_infra}\n"
                + _diff_lines(
                    ch_canon,
                    fa_canon,
                    "Chalice",
                    "FastAPI",
                )
            )
