"""Status routes — health checks and version endpoint.

Ported from: backend/chalicelib/blueprints/status.py
"""

import sqlalchemy
from fastapi import APIRouter

from chalicelib.__version__ import __version__
from chalicelib.schema import engine
from exceptions import ChaliceViewError

router = APIRouter(tags=["Status"])


@router.get("/health/api")
def health():
    return {"status": "ok"}


@router.get("/health/db")
def health_db():
    try:
        conn = engine.connect()
    except sqlalchemy.exc.OperationalError as e:
        raise ChaliceViewError(str(e)) from e
    else:
        conn.close()
        return {"status": "ok"}


@router.get("/version")
def version():
    return {"version": __version__}
