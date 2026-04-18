from dataclasses import dataclass
from typing import Optional

from chalice import Blueprint

from chalicelib.new.config.infra import envars

from . import (
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
    pasto,
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

# Import dev_auth only in local mode
if envars.LOCAL_INFRA:
    from . import dev_auth


@dataclass
class Route:
    prefix: str | None
    blueprint: Blueprint


blueprints_collection: list[Route] = [
    Route("/status", status.bp),
    Route("/Company", company.bp),
    Route("/CFDI", cfdi.bp),
    Route("/Export", cfdi_export.bp),
    Route("/DoctoRelacionado", docto_relacionado.bp),
    Route("/CFDIExcluded", cfdi_excluded.bp),
    Route("/EFOS", efos.bp),
    Route("/License", license_bp.bp),
    Route("/Notification", notification.bp),
    Route("/Param", param.bp),
    Route("/Permission", permission.bp),
    Route("/Poliza", poliza.bp),
    Route("/Product", product.bp),
    Route("/RegimenFiscal", regimen_fiscal.bp),
    Route("/SATQuery", sat_query.bp),
    Route("/User", user.bp),
    Route("/Workspace", workspace.bp),
    Route("/Pasto/Worker", pasto.worker.bp),
    Route("/Pasto/Sync", pasto.sync.bp),
    Route("/Pasto/ResetLicense", pasto.reset.bp),
    Route("/Pasto/Company", pasto.company.bp),
    Route("/", pasto.config.bp),
    Route("/", pasto.metadata.bp),
    Route("/", pasto.xml.bp),
    Route("/", pasto.cancel.bp),
    Route("/Scraper", scraper.bp),
    Route("/Attachment", attachment.bp),
    Route("/COI", coi.bp),
]

# Add dev auth routes only in local mode
if envars.LOCAL_INFRA:
    blueprints_collection.insert(0, Route("/", dev_auth.blueprint))
