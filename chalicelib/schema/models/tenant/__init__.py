"""
Tenant models and catalogs namespace.
This module ensures all tenant-specific models and catalog foreign tables
are available in the same namespace.
"""

# Import all tenant catalog models to make them available in relationships
from sqlalchemy import and_
from sqlalchemy.orm import aliased, foreign, relationship, remote

from chalicelib.schema.models.catalogs import *  # noqa: F403
from chalicelib.schema.models.catalogs import CatFormaPago, CatRegimenFiscal
from chalicelib.schema.models.tenant.cfdi_relacionado import CfdiRelacionado

from .cfdi import CFDI
from .docto_relacionado import DoctoRelacionado
from .nomina import Nomina
from .payment import Payment
from .poliza import Poliza
from .poliza_cfdi import PolizaCFDI
from .sat_query import SATQuery

DoctoRelacionado.cfdi_origin.alias = aliased(CFDI, name="cfdi_origin", flat=True)

DoctoRelacionado.cfdi_related.alias = aliased(CFDI, name="cfdi_related", flat=True)

CFDI.active_payments.alias = aliased(CFDI, name="active_payments", flat=True)
CFDI.c_regimen_fiscal_emisor.alias = aliased(
    CatRegimenFiscal, name="c_regimen_fiscal_emisor", flat=True
)
CFDI.c_regimen_fiscal_receptor.alias = aliased(
    CatRegimenFiscal, name="c_regimen_fiscal_receptor", flat=True
)
Payment.c_forma_pago.alias = aliased(CatFormaPago, name="c_forma_pago", flat=True)
CFDI.pays.alias = aliased(DoctoRelacionado, name="pays", flat=True)
CFDI.paid_by.alias = aliased(DoctoRelacionado, name="paid_by", flat=True)
CFDI.c_forma_pago.alias = aliased(CatFormaPago, name="c_forma_pago", flat=True)


cfdi_origin: CFDI = aliased(CFDI, name="cfdi_origin", flat=True)

CFDI.active_egresos = relationship(  # TODO check if used, duplicado de CFDI.cfdi_related
    cfdi_origin,
    secondary=CfdiRelacionado.__table__,
    foreign_keys=[CfdiRelacionado.uuid_origin, CfdiRelacionado.uuid_related],
    primaryjoin=and_(
        foreign(CfdiRelacionado.uuid_related) == CFDI.UUID,
        CfdiRelacionado.TipoDeComprobante == "E",
    ),
    secondaryjoin=and_(
        foreign(CfdiRelacionado.uuid_origin) == remote(cfdi_origin.UUID),
        cfdi_origin.Estatus,
    ),
    viewonly=True,
    lazy="joined",  # carga en el mismo query
    innerjoin=True,  # fuerza un JOIN explícito
)
CFDI.active_egresos.alias = cfdi_origin
CfdiRelacionado.cfdi_related.alias = aliased(CFDI, name="cfdi_related", flat=True)
