"""FastAPI application entry point — replaces Chalice's app.py for HTTP routes."""

import os
from decimal import Decimal

from dotenv import load_dotenv

load_dotenv()

from fastapi import FastAPI, Request
from fastapi.encoders import ENCODERS_BY_TYPE
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

ENCODERS_BY_TYPE[Decimal] = float

from chalicelib.new.config.infra import envars
from exceptions import (
    BadRequestError,
    ChaliceViewError,
    ForbiddenError,
    MethodNotAllowedError,
    NotFoundError,
    UnauthorizedError,
)
from routers import (
    attachment,
    cfdi,
    cfdi_excluded,
    cfdi_export,
    coi,
    company,
    docto_relacionado,
    efos,
    license_bp,
    notification,
    param,
    permission,
    poliza,
    product,
    regimen_fiscal,
    sat_query,
    scraper,
    status,
    user,
    workspace,
)
from routers.pasto import cancel, config, metadata, reset, sync, worker, xml
from routers.pasto import company as pasto_company

if envars.LOCAL_INFRA:
    from routers import dev_auth

app = FastAPI(
    title="solucioncp-backend",
    version="40.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)


# ---------------------------------------------------------------------------
# Startup event — initialize EventBus handlers for SQS/async processing
# ---------------------------------------------------------------------------
@app.on_event("startup")
async def startup_event():
    """Initialize EventBus handlers on application startup.

    This registers all SQS handlers for async event processing (exports, queries, etc.).
    Without this, bus.publish() calls will log NO_HANDLERS and events won't be processed.
    """
    from chalicelib.bus import suscribe_all_handlers

    suscribe_all_handlers()

# ---------------------------------------------------------------------------
# CORS — mirrors Chalice add_cors_headers middleware
# ---------------------------------------------------------------------------
if envars.LOCAL_INFRA:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:5173"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["Content-Type", "access_token", "authorization"],
        expose_headers=["*"],
        max_age=86400,
    )
else:
    _cors_raw = (os.environ.get("FASTAPI_CORS_ORIGINS") or "").strip()
    if _cors_raw:
        _origins = [x.strip() for x in _cors_raw.split(",") if x.strip()]
        app.add_middleware(
            CORSMiddleware,
            allow_origins=_origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
            expose_headers=["*"],
            max_age=86400,
        )

# ---------------------------------------------------------------------------
# Global exception handlers — all errors return Chalice-compatible shape:
#   {"Code": "<ErrorClass>", "Message": "<detail>"}
# The frontend reads error.response.data.Message everywhere.
# ---------------------------------------------------------------------------
_BRIDGE_EXCEPTIONS = (
    BadRequestError,
    UnauthorizedError,
    ForbiddenError,
    NotFoundError,
    MethodNotAllowedError,
    ChaliceViewError,
)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    """Convert FastAPI's 422 validation errors into Chalice-shaped responses.

    Missing access_token header → 401 (Chalice would raise UnauthorizedError).
    Everything else → 400 BadRequestError with a human-readable message.
    """
    for err in exc.errors():
        loc = err.get("loc", ())
        if len(loc) >= 2 and loc[0] == "header" and loc[1] == "access_token":
            return JSONResponse(
                status_code=401,
                content={"Code": "UnauthorizedError", "Message": "Unauthorized"},
            )
    messages = "; ".join(
        f"{'.'.join(str(part) for part in e.get('loc', ()))}: {e.get('msg', '')}"
        for e in exc.errors()
    )
    return JSONResponse(
        status_code=400,
        content={"Code": "BadRequestError", "Message": messages},
    )


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException) -> JSONResponse:
    if isinstance(exc, _BRIDGE_EXCEPTIONS):
        return JSONResponse(
            status_code=exc.status_code,
            content={"Code": type(exc).__name__, "Message": exc.detail},
        )
    return JSONResponse(
        status_code=exc.status_code,
        content={"Code": "HTTPException", "Message": exc.detail},
    )


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(
        status_code=500,
        content={"Code": "InternalServerError", "Message": str(exc)},
    )


# ---------------------------------------------------------------------------
# Router mounts — all under /api to match Chalice api_gateway_stage
# ---------------------------------------------------------------------------
app.include_router(status.router, prefix="/api/status")
app.include_router(user.router, prefix="/api/User")
app.include_router(company.router, prefix="/api/Company")
app.include_router(workspace.router, prefix="/api/Workspace")
app.include_router(permission.router, prefix="/api/Permission")
app.include_router(cfdi.router, prefix="/api/CFDI")
app.include_router(cfdi_export.router, prefix="/api/Export")
app.include_router(cfdi_excluded.router, prefix="/api/CFDIExcluded")
app.include_router(docto_relacionado.router, prefix="/api/DoctoRelacionado")
app.include_router(efos.router, prefix="/api/EFOS")
app.include_router(license_bp.router, prefix="/api/License")
app.include_router(notification.router, prefix="/api/Notification")
app.include_router(param.router, prefix="/api/Param")
app.include_router(poliza.router, prefix="/api/Poliza")
app.include_router(product.router, prefix="/api/Product")
app.include_router(regimen_fiscal.router, prefix="/api/RegimenFiscal")
app.include_router(sat_query.router, prefix="/api/SATQuery")
app.include_router(scraper.router, prefix="/api/Scraper")
app.include_router(attachment.router, prefix="/api/Attachment")
app.include_router(coi.router, prefix="/api/COI")
app.include_router(worker.router, prefix="/api/Pasto/Worker")
app.include_router(sync.router, prefix="/api/Pasto/Sync")
app.include_router(reset.router, prefix="/api/Pasto/ResetLicense")
app.include_router(pasto_company.router, prefix="/api/Pasto/Company")
app.include_router(config.router, prefix="/api")
app.include_router(metadata.router, prefix="/api")
app.include_router(xml.router, prefix="/api")
app.include_router(cancel.router, prefix="/api")

if envars.LOCAL_INFRA:
    app.include_router(dev_auth.router, prefix="/api")
