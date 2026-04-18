from sqlalchemy.orm import Session

from chalicelib.controllers.cfdi import CFDIController
from chalicelib.new.cfdi_processor.domain.xlsx_exporter import process_iterable
from chalicelib.new.model_serializer.app.model_serializer import ModelSerializer
from chalicelib.schema.models.tenant.cfdi import CFDI
from chalicelib.schema.models.tenant.cfdi_relacionado import CfdiRelacionado


def test_reproduce_attribute_error(company_session: Session):
    cfdi = CFDI.demo()
    cfdi_2 = CFDI.demo()
    cfdi_relacionado = CfdiRelacionado(
        uuid_origin=cfdi.UUID,  # No es relevante para el test
        TipoRelacion="01",  # No es relevante para el test
        uuid_related=cfdi.UUID,
        TipoDeComprobante="E",
        Estatus=True,
    )
    cfdi_relacionado_2 = CfdiRelacionado(
        uuid_origin=cfdi_2.UUID,  # No es relevante para el test
        TipoRelacion="01",  # No es relevante para el test
        uuid_related=cfdi.UUID,
        TipoDeComprobante="E",
        Estatus=True,
    )
    company_session.add_all([cfdi, cfdi_relacionado, cfdi_relacionado_2])
    company_session.flush()

    fields = [
        "Fecha",
        "Serie",
        "Folio",
        "RfcReceptor",
        "NombreReceptor",
        "Total",
        "balance",
        "paid_by.UUID",
        "cfdi_related.uuid_origin",
        "SubTotal",
        "Descuento",
        "Neto",
        "TrasladosIVA",
        "UsoCFDIReceptor",
        "MetodoPago",
        "FormaPago",
        "payments.FechaPago",
        "payments.NumOperacion",
        "payments.NomBancoOrdExt",
        "payments.RfcEmisorCtaOrd",
        "payments.CtaOrdenante",
        "payments.RfcEmisorCtaBen",
        "payments.CtaBeneficiario",
        "payments.MonedaP",
        "payments.c_forma_pago.name",
        "payments.TipoCambioP",
        "payments.Monto",
    ]

    query = CFDIController._search(  # pylint: disable=protected-access
        domain=[],
        fields=fields,
        lazzy=True,
        session=company_session,
    )

    serializer = ModelSerializer(process_iterable=process_iterable)
    data = []
    for record in query:
        data.append(serializer.serialize(record, fields))
    pass


def test_serialize_cfdi_object():
    serializer = ModelSerializer()
    cfdi = CFDI.demo()
    fields = [
        "Fecha",
        "Serie",
        "Folio",
    ]
    serialized = serializer.serialize(cfdi, fields)
    assert set(serialized.keys()) == set(fields)
