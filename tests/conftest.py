from __future__ import annotations

import os
import sys
from pathlib import Path

# Monorepo: expose legacy Chalice `app` for tests that use `chalice.test.Client`.
_fastapi_root = Path(__file__).resolve().parent.parent
_monorepo_root = _fastapi_root.parent
_backend_app = _monorepo_root / "backend" / "app.py"
if _backend_app.is_file():
    _backend_dir = str(_monorepo_root / "backend")
    if _backend_dir not in sys.path:
        sys.path.insert(0, _backend_dir)

# Full AWS/DB fixture graph (autouse Cognito/S3/SQS). Skip for lightweight tests
# (e.g. HTTP parity, golden JSON) via: PYTEST_MINIMAL_CONTEST=1 poetry run pytest …
if os.environ.get("PYTEST_MINIMAL_CONTEST") != "1":
    from tests.fixtures.factories.user import *  # noqa
    from tests.fixtures.factories.company import *  # noqa
    from tests.fixtures.factories.pasto import *  # noqa
    from tests.fixtures.factories.permission import *  # noqa
    from tests.fixtures.factories.workspace import *  # noqa
    from tests.fixtures.auth import *  # noqa
    from tests.fixtures.aws import *  # noqa
    from tests.fixtures.db import *  # noqa
    from tests.fixtures.company import *  # noqa
    from tests.fixtures.bus import *  # noqa
    from tests.fixtures.data import *  # noqa


def pytest_addoption(parser):
    parser.addoption(
        "--cid",
        action="store",
        default="",
        help="CID para la fixture de company",
    )
    parser.addoption(
        "--commit",
        action="store_true",
        help="Si se pasa, se realiza commit a la BD",
    )
