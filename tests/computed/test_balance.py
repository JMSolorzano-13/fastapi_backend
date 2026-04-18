import random

from sqlalchemy.orm import Session

from chalicelib.schema.models.tenant.cfdi import CFDI
from chalicelib.schema.models.tenant.docto_relacionado import DoctoRelacionado


def test_fis_769_balance_Ingreso_PUE_eq_0(company_session: Session):
    total = random.randint(10, 1000)
    cfdi = CFDI.demo(
        TipoDeComprobante="I",
        MetodoPago="PUE",
        Total=total,
    )
    company_session.add(cfdi)
    company_session.flush()
    assert cfdi.balance == 0


def test_fis_769_balance_Ingreso_PUE_eq_neg_if_docto(company_session: Session):
    total = random.randint(10, 1000)
    cfdi = CFDI.demo(
        TipoDeComprobante="I",
        MetodoPago="PUE",
        Total=total,
    )
    total_paid = random.randint(1, total)
    docto_relacionado = DoctoRelacionado.demo(
        UUID_related=cfdi.UUID,
        ImpPagado=total_paid,
    )
    company_session.add_all(
        [
            cfdi,
            docto_relacionado,
        ]
    )
    company_session.flush()
    assert cfdi.balance == -total_paid


def test_fis_769_balance_no_Ingreso_PUE_eq_total(company_session: Session):
    total = random.randint(10, 1000)
    cfdi = CFDI.demo(
        TipoDeComprobante="I",
        MetodoPago="PPD",
        Total=total,
    )
    company_session.add(cfdi)
    company_session.flush()
    assert cfdi.balance == total


def test_fis_769_balance_no_Ingreso_PUE_sub_payments(company_session: Session):
    total = random.randint(10, 1000)
    cfdi = CFDI.demo(
        TipoDeComprobante="I",
        MetodoPago="PPD",
        Total=total,
    )
    docto_1 = DoctoRelacionado.demo(
        UUID_related=cfdi.UUID,
        ImpPagado=total // 2,
    )
    docto_2 = DoctoRelacionado.demo(
        UUID_related=cfdi.UUID,
        ImpPagado=total - docto_1.ImpPagado,
    )
    company_session.add_all([cfdi, docto_1, docto_2])
    company_session.flush()
    assert cfdi.balance == 0


def test_fis_769_balance_no_Ingreso_PUE_ignore_canceled(company_session: Session):
    total = random.randint(10, 1000)
    cfdi = CFDI.demo(
        TipoDeComprobante="I",
        MetodoPago="PPD",
        Total=total,
    )
    docto_1 = DoctoRelacionado.demo(
        UUID_related=cfdi.UUID,
        ImpPagado=total // 2,
    )
    docto_2 = DoctoRelacionado.demo(
        UUID_related=cfdi.UUID,
        ImpPagado=total - docto_1.ImpPagado,
        Estatus=False,
    )
    company_session.add_all([cfdi, docto_1, docto_2])
    company_session.flush()
    assert cfdi.balance == total - docto_1.ImpPagado
