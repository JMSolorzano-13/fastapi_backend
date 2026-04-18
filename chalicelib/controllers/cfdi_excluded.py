from types import SimpleNamespace

from sqlalchemy import and_, literal, or_
from sqlalchemy.orm import Query, Session
from sqlalchemy.orm.attributes import InstrumentedAttribute

from chalicelib.controllers import Domain, get_filters
from chalicelib.controllers.common import CommonController, FieldsLabeled
from chalicelib.schema.models.model import Model
from chalicelib.schema.models.tenant import CFDI, DoctoRelacionado, Payment

# Alias para legibilidad
_cfdi_rel = DoctoRelacionado.cfdi_related.alias
_pago_rel = Payment.c_forma_pago.alias

_docto_filter_ns = SimpleNamespace(
    ExcludeFromIVA=DoctoRelacionado.ExcludeFromIVA,
    Estatus=DoctoRelacionado.Estatus,
    is_issued=_cfdi_rel.is_issued,
    Version=_cfdi_rel.Version,
    TipoDeComprobante=_cfdi_rel.TipoDeComprobante,
    PaymentDate=Payment.FechaPago,
    Fecha=_cfdi_rel.Fecha,
    MetodoPago=DoctoRelacionado.MetodoDePagoDR,
)

_PUSHDOWN_FIELDS = frozenset(
    {
        "ExcludeFromIVA",
        "Estatus",
        "is_issued",
        "Version",
        "TipoDeComprobante",
        "PaymentDate",
        "Fecha",
        "MetodoPago",
    }
)


def _pushdown_fields_of(item) -> frozenset[str]:
    """Extrae nombres de campo de un item del domain (condición simple o bloque OR)."""
    if not isinstance(item, (list, tuple)):
        return frozenset()
    # Condición simple: ["field", "op", "value"]
    if len(item) == 3 and isinstance(item[0], str):
        return frozenset({item[0]})
    # Bloque OR: ["|", [[grupo_1], [grupo_2], ...]]
    if len(item) == 2 and item[0] == "|" and isinstance(item[1], list):
        fields: set[str] = set()
        for branch in item[1]:
            if isinstance(branch, (list, tuple)):
                if len(branch) == 3 and isinstance(branch[0], str):
                    fields.add(branch[0])
                else:
                    for sub in branch:
                        if isinstance(sub, (list, tuple)) and len(sub) == 3:
                            fields.add(sub[0])
        return frozenset(fields)
    return frozenset()


class ExcludedCFDIController(CommonController):
    model = CFDI
    _order_by = "PaymentDate"
    subquery = None

    # Campos para UNION: [CFDI_field, DoctoRelacionado_field]
    _UNION_FIELDS = lambda: [  # noqa: E731
        [CFDI.company_identifier, DoctoRelacionado.company_identifier],
        [CFDI.Estatus, and_(_cfdi_rel.Estatus, DoctoRelacionado.Estatus)],
        [CFDI.is_issued, _cfdi_rel.is_issued],
        [CFDI.Version, _cfdi_rel.Version],
        [CFDI.ExcludeFromIVA, DoctoRelacionado.ExcludeFromIVA],
        [CFDI.Fecha, _cfdi_rel.Fecha],
        [CFDI.PaymentDate, Payment.FechaPago],
        [CFDI.UUID, _cfdi_rel.UUID],
        [CFDI.Serie, DoctoRelacionado.Serie],
        [CFDI.Folio, DoctoRelacionado.Folio],
        [CFDI.RfcEmisor, _cfdi_rel.RfcEmisor],
        [CFDI.NombreEmisor, _cfdi_rel.NombreEmisor],
        [CFDI.TipoDeComprobante, _cfdi_rel.TipoDeComprobante],
        [CFDI.UsoCFDIReceptor, _cfdi_rel.UsoCFDIReceptor],
        [CFDI.FormaPago, Payment.FormaDePagoP],
        [
            CFDI.c_forma_pago.alias.name.label("FormaPagoName"),
            _pago_rel.name.label("FormaPagoName"),
        ],
        [CFDI.MetodoPago, _cfdi_rel.MetodoPago],
        [CFDI.BaseIVA16, DoctoRelacionado.BaseIVA16],
        [CFDI.BaseIVA8, DoctoRelacionado.BaseIVA8],
        [CFDI.BaseIVA0, DoctoRelacionado.BaseIVA0],
        [CFDI.BaseIVAExento, DoctoRelacionado.BaseIVAExento],
        [CFDI.IVATrasladado16, DoctoRelacionado.IVATrasladado16],
        [CFDI.IVATrasladado8, DoctoRelacionado.IVATrasladado8],
        [
            CFDI.TrasladosIVAMXN.label("TrasladosIVA"),
            DoctoRelacionado.TrasladosIVAMXN.label("TrasladosIVA"),
        ],
        [
            CFDI.RetencionesIVAMXN.label("RetencionesIVA"),
            DoctoRelacionado.RetencionesIVAMXN.label("RetencionesIVA"),
        ],
        [CFDI.TotalMXN.label("Total"), DoctoRelacionado.ImpPagadoMXN.label("Total")],
        [literal(None).label("DR-UUID"), DoctoRelacionado.UUID],
        [literal(None).label("DR-Identifier"), DoctoRelacionado.identifier],
    ]

    @classmethod
    def _get_query_model(cls, session: Session, fields: FieldsLabeled, domain: Domain = None):
        union_fields = cls._UNION_FIELDS()
        for t in union_fields:
            t[0] = t[0].label(t[0].name)

        cfdi_fields, docto_fields = zip(*union_fields, strict=True)

        pushdown = [
            item
            for item in domain or []
            if (fields := _pushdown_fields_of(item)) and fields <= _PUSHDOWN_FIELDS
        ]

        cfdi_query = (
            session.query(*cfdi_fields)
            .outerjoin(CFDI.c_forma_pago.alias, CFDI.c_forma_pago)
            .filter(
                or_(CFDI.MetodoPago == "PUE", CFDI.MetodoPago is None),
                *(get_filters(CFDI, pushdown, session) if pushdown else ()),
            )
        )

        docto_query = (
            session.query(*docto_fields)
            .join(_cfdi_rel, DoctoRelacionado.cfdi_related)
            .join(DoctoRelacionado.payment_related)
            .outerjoin(_pago_rel, Payment.c_forma_pago)
            .filter(*(get_filters(_docto_filter_ns, pushdown, session) if pushdown else ()))
        )

        return cfdi_query.union_all(docto_query)

    @classmethod
    def apply_domain(
        cls,
        query,
        domain: Domain,
        fuzzy_search: str = "",
        *,
        join_fields: list[tuple[Model, InstrumentedAttribute]] | None = None,
        session,
    ):
        cls.subquery = query.subquery()
        cls.model = cls.subquery.c  # type: ignore
        query = session.query(cls.subquery)  # type: ignore

        # Aplicar filtros a la subquery (redundantes pero seguros)
        # Los filtros pushdown ya optimizan cada parte del UNION internamente
        filters = get_filters(cls.model, domain, session)
        if filters is not None:
            query = query.filter(*filters)

        cls.model = CFDI  # type: ignore

        if fuzzy_search:
            query = cls._fuzzy_search(query, fuzzy_search)

        return query

    @classmethod
    def _apply_order_by(cls, model: type[Model], order_by: str, query: Query) -> Query:
        """Apply ordering to query based on order_by specification."""
        table_name = model.__table__.name
        order_by = order_by.replace(f"{table_name}.", "").replace('"', "")
        model = cls.subquery.c  # type: ignore

        for part in order_by.split(","):
            column, order_mode = CommonController._parse_order_specification(part)
            attribute, join_fields = CommonController._build_order_attribute(
                model, column, order_mode
            )
            query = query.order_by(attribute)

        return query

    @classmethod
    def apply_active_filter_if_needed(cls, query: Query, active_value: bool) -> None:
        return query
