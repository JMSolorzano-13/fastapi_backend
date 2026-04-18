import enum
import functools
import io
import json
from collections import OrderedDict, defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from tempfile import NamedTemporaryFile
from typing import Any
from zipfile import ZipFile

import unidecode
from chalice import ForbiddenError, NotFoundError, UnauthorizedError
from chalice.app import MethodNotAllowedError
from openpyxl import Workbook  # type: ignore
from sqlalchemy import (  # type: ignore
    String,
    Text,
    and_,
    case,
    cast,
    func,
    literal,
    nulls_first,
    nulls_last,
    or_,
    select,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB  # type: ignore
from sqlalchemy.orm import Query, Session, aliased
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.sql.functions import ReturnTypeFromArgs, coalesce

from chalicelib.boto3_clients import s3_client
from chalicelib.controllers import (
    Domain,
    DomainElement,
    SearchResult,
    SearchResultPaged,
    ensure_list,
    ensure_set,
    get_alias_or_model,
    get_filters,
    is_super_user,
    is_x2m,
)
from chalicelib.controllers.common_utils import export_xlsx
from chalicelib.logger import DEBUG, WARNING, log, log_in
from chalicelib.modules import Modules, NameEnum
from chalicelib.new.cfdi_processor.domain.xlsx_exporter import (
    XLSXExporter,
    process_iterable,
)
from chalicelib.new.config.infra import envars
from chalicelib.new.model_serializer.app.model_serializer import ModelSerializer
from chalicelib.new.shared.domain.primitives import (
    Identifier,
    identifier_default_factory,
)
from chalicelib.schema.models import (  # pylint: disable=no-name-in-module
    Company,
    Model,
    Permission,
    User,
    Workspace,
)
from chalicelib.schema.models.tenant import DoctoRelacionado as DoctoRelacionadoORM
from chalicelib.schema.models.tenant.cfdi import CFDI
from chalicelib.schema.models.tenant.cfdi_export import CfdiExport
from chalicelib.schema.models.tenant.cfdi_relacionado import CfdiRelacionado
from chalicelib.schema.models.tenant.tenant_model import TenantBaseModel

FieldsLabeled = dict[str, str]
EXPORT_EXPIRATION = 60 * 60 * 24 * 7

PrimitiveType = str | int | float | bool | date | datetime

FUZZY_CHAR_SEPARATOR = text("'🍔'")


def json_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, date | datetime):
        return obj.isoformat()
    if isinstance(obj, enum.Enum):
        return obj.name
    try:
        return str(obj)
    except Exception as e:
        raise TypeError(f"Type {type(obj)} not serializable, value: {obj}") from e


@dataclass
class FieldInfo:
    original_key: str
    label: str
    position: int
    relation_path: str
    field_path: str


@dataclass
class ColumnSpec:
    column_expr: Any
    label: str
    position: int
    is_subquery: bool


class ColumnsNameExcel(enum.Enum):
    Fecha = "Fecha"
    Version = "Versión"
    Serie = "Serie"
    Folio = "Folio"
    RfcReceptor = "Rfc Receptor"
    NombreReceptor = "Nombre Receptor"
    RfcEmisor = "Rfc Emisor"
    NombreEmisor = "Nombre Emisor"
    RegimenFiscalReceptor = "Régimen Fiscal Receptor"
    SubTotal = "Subtotal"
    Descuento = "Descuento"
    Neto = "Neto"
    RetencionesIVA = "Retenciones IVA"
    RetencionesISR = "Retenciones ISR"
    TrasladosIVA = "Traslados IVA"
    Total = "Total"
    TotalMXN = "Total MXN"
    Moneda = "Moneda"
    TipoCambio = "Tipo Cambio"
    UsoCFDIReceptor = "Uso CFDI Receptor"
    FormaPago = "Forma de Pago"
    MetodoPago = "Método de Pago"
    CondicionesDePago = "Condiciones De Pago"
    FechaCertificacionSat = "Fecha de certificación"
    RetencionesIEPS = "Retenciones IEPS"
    TrasladosIEPS = "Traslados IEPS"
    TrasladosISR = "Traslados ISR"
    NoCertificado = "No Certificado"
    TipoDeComprobante = "Tipo de Comprobante"
    Exportacion = "Exportacion"
    Periodicidad = "Periodicidad"
    Meses = "Meses"
    CfdiRelacionados = "Tipos de relación y UUIDs relacionados"
    LugarExpedicion = "Lugar Expedicion"
    UUID = "UUID"
    balance = "Saldo"


class ExportFormat(NameEnum):
    PDF = enum.auto()
    XLSX = enum.auto()
    XML = enum.auto()


class unaccent(ReturnTypeFromArgs):  # pylint: disable=too-many-ancestors
    inherit_cache = True


def _plain_field(record: Model, field_str: str) -> Any:
    res = record
    for part in field_str.split("."):
        if not res:
            continue
        res = getattr(res, part)
    return CommonController._to_primitive(res)  # pylint: disable=protected-access


def check_context(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        context = kwargs.get("context", {}) or {}
        kwargs["context"] = context
        res = f(*args, **kwargs)
        if not is_super_user(context) and not context.get("guest_user"):  # TODO
            cls = args[0]
            session = kwargs["session"]
            cls.check_companies(res, session=session, context=context)
        return res

    return wrapper


custom_columns = (
    "PaymentDate",
    "BaseIVA16",
    "BaseIVA8",
    "BaseIVA0",
    "IVATrasladado16",
    "IVATrasladado8",
    "TrasladosIVA",
    "RetencionesIVA",
    "Total",
    "FormaPago",
    "Serie",
    "Folio",
)

resume_fields = (
    "Tipo",
    "Conteo de CFDIs",
    "Retención IVA",
    "Retención IEPS",
    "Retención ISR",
    "Traslado IVA",
    "Traslado IEPS",
    "Traslado ISR",
    "Impuesto Local",
    "Subtotal",
    "Descuento",
    "Neto",
    "Total",
)


class CommonController:
    model: type[Model] | type[TenantBaseModel]

    restricted_fields = {
        "id",
        "created_at",
        "updated_at",
    }
    restricted_update_fields = {
        "id",
        "created_at",
        "updated_at",
    }
    default_distinct = False

    fuzzy_fields: tuple[Any, ...] = ()
    _order_by: str = ""

    @classmethod
    def get_controllers_by_model(cls) -> dict[type[Model], type["CommonController"]]:
        controllers = CommonController.__subclasses__()
        return {getattr(controller, "model", None): controller for controller in controllers}  # type: ignore

    @classmethod
    def _join_query_and_models_from_fields(
        cls,
        query: Query,
        relation_models_and_fields: list[tuple[Model, InstrumentedAttribute]],
    ) -> Query:
        for join_tuple in relation_models_and_fields:
            # Se usa `isouter` para obtener los registros aunque no tengan un
            # elemento para hacer el join, util en el `SELECT` (left join)
            query = query.join(*join_tuple, isouter=True)
        return query

    @classmethod
    def _fuzzy_search(cls, query, fuzzy_search):
        if not envars.control.FUZZY_SEARCH_ACTIVE:
            log(
                Modules.SEARCH,
                WARNING,
                "FUZZY_SEARCH_INACTIVE",
            )
            return query
        if not fuzzy_search:
            return query

        def combine_fields(fields):
            fuzzy_fields_combined = FUZZY_CHAR_SEPARATOR
            for field in fields:
                fuzzy_fields_combined += (
                    coalesce(cast(field, Text), FUZZY_CHAR_SEPARATOR) + FUZZY_CHAR_SEPARATOR
                )
            return fuzzy_fields_combined  # '🍔' + coalesce(field, '🍔') + '🍔'

        fuzzy_search = unidecode.unidecode(fuzzy_search)

        fuzzy_domains_per_model = (combine_fields(fields) for fields in (cls.fuzzy_fields,))

        fuzzy_filters = tuple(
            unaccent(fuzzy_part).ilike(f"%{fuzzy_search}%")
            for fuzzy_part in fuzzy_domains_per_model
        )
        return query.filter(
            or_(*fuzzy_filters),
        )

    @classmethod
    def _get_default_order_by(cls, *, session: Session):
        order_by = ""
        if cls._order_by:
            order_by = cls._order_by
        elif "name" in cls.model.__table__.c:
            order_by = "name"
        elif "id" in cls.model.__table__.c:
            order_by = "id"
        elif "identifier" in cls.model.__table__.c:
            order_by = "identifier"
        return order_by

    @staticmethod
    def get_custom_order_by(scaped, column, order_mode):
        if order_mode == "ASC":
            scaped.append(f'"{column}" {order_mode} NULLS FIRST')
        else:
            scaped.append(f'"{column}" {order_mode} NULLS LAST')

    @staticmethod
    def _parse_order_specification(part: str) -> tuple[str, str]:
        """Parse a single order specification into column and direction."""
        components = part.strip().split(" ")
        column = components[0]
        order_mode = components[1].upper() if len(components) > 1 else "ASC"
        return column, order_mode

    @staticmethod
    def _build_order_attribute(
        model: type[Model], column_path: str, order_mode: str
    ) -> tuple[Any, list[tuple[Model, InstrumentedAttribute]]]:
        """Build the SQLAlchemy order attribute for a given column path."""
        final_field, join_fields = CommonController._field_str_to_field_and_joins(
            model, column_path
        )
        # Apply ordering and null handling
        null_order = nulls_first if order_mode == "ASC" else nulls_last
        attribute = final_field.asc() if order_mode == "ASC" else final_field.desc()
        attribute = null_order(attribute)

        return attribute, join_fields

    @classmethod
    def _apply_order_by(cls, model: type[Model], order_by: str, query: Query) -> Query:
        """Apply ordering to query based on order_by specification."""
        table_name = model.__table__.name
        order_by = order_by.replace(f"{table_name}.", "").replace('"', "")

        for part in order_by.split(","):
            column, order_mode = CommonController._parse_order_specification(part)
            attribute, join_fields = CommonController._build_order_attribute(
                model, column, order_mode
            )

            query = query.order_by(attribute)
            if join_fields:
                query = CommonController._join_query_and_models_from_fields(query, join_fields)

        return query

    @classmethod
    def _get_relational_fields(
        cls, model, composed_fields: list
    ) -> list[Any]:  # TODO remove cuando se limpien las exportaciones
        composed_fields = (
            composed_fields if isinstance(composed_fields, list) else [composed_fields]
        )
        relational_fields = []
        fields = []
        for composed_field in composed_fields:
            parts = composed_field.split(".")
            if len(parts) == 1:
                continue
            local_model = model
            for part in parts:
                column = getattr(local_model, part, None)
                if column and hasattr(column, "property") and hasattr(column.property, "mapper"):
                    local_model = get_alias_or_model(column)
                    relational_fields.append((local_model, column))
                else:
                    fields.append(column.label(composed_field))
        return relational_fields, fields

    @classmethod
    def balance_query_sql(cls, company_identifier: Identifier) -> str:
        subquery = (
            select(
                [
                    func.coalesce(
                        func.round(
                            func.sum(DoctoRelacionadoORM.ImpPagado),
                            2,
                        ),
                        0,
                    )
                ]
            )
            .where(
                and_(
                    DoctoRelacionadoORM.UUID_related == cls.model.UUID,
                    DoctoRelacionadoORM.Estatus,
                )
            )
            .correlate(cls.model.__table__)
            .scalar_subquery()
        )

        query = case(
            [
                (
                    and_(cls.model.TipoDeComprobante == "I", cls.model.MetodoPago == "PUE"),
                    0 - subquery,
                )
            ],
            else_=(cls.model.Total or 0) - subquery,
        )
        return query

    @classmethod
    def get_balance_query_sql_with_cid(cls, body, base_fields):
        company_identifier = body.get("domain")[0][2]
        balance_query_sql = cls.balance_query_sql(company_identifier)

        base_fields.append(balance_query_sql)
        return balance_query_sql

    @classmethod
    def get_egresos_relacionados_query(cls, model, query, body):
        cfdi_egreso_alias = aliased(model, name="cfdi_1")

        query = query.outerjoin(
            CfdiRelacionado,
            and_(
                CfdiRelacionado.uuid_related == model.UUID,
                CfdiRelacionado.Estatus == True,
                CfdiRelacionado.TipoDeComprobante == "E",
            ),
        )

        query = query.outerjoin(
            cfdi_egreso_alias,
            and_(
                CfdiRelacionado.uuid_origin == cfdi_egreso_alias.UUID,
                cfdi_egreso_alias.TipoDeComprobante == "E",
                cfdi_egreso_alias.Estatus == True,
            ),
        )

        uuid_origin_column = func.string_agg(
            func.distinct(func.cast(CfdiRelacionado.uuid_origin, String)), ", "
        )

        # Bug fix: SUM(DISTINCT Total) descartaba egresos con el mismo monto.
        # Se usa SUM sin DISTINCT y se suma TotalMXN (no Total) para que el
        # resultado sea siempre en pesos independientemente de la moneda del egreso.
        total_egresos_column = func.sum(cfdi_egreso_alias.TotalMXN)

        return uuid_origin_column, total_egresos_column, query

    @classmethod
    def get_query(  # TODO remove cuando se limpien las exportaciones
        cls,
        model,
        fields: list,
        body,
        aggregate: bool = False,
        *,
        sql_query: Query,
    ) -> Query:
        base_fields = []

        # dict for special fields to join and alias
        # important for fields with relations the same table appear more than once
        special_fields = {
            "c_regimen_fiscal_emisor.name": ("c_regimen_fiscal_emisor", "RegimenFiscalEmisor"),
            "c_regimen_fiscal_receptor.name": (
                "c_regimen_fiscal_receptor",
                "RegimenFiscalReceptor",
            ),
        }
        # Skips for aggregate queries
        skip_aggregate_fields = {
            "c_moneda.name",
            "payments.c_forma_pago.name",
            "payments.TipoCambioP",
            "payments.FormaDePagoP",
            "payments.RfcEmisorCtaOrd",
            "payments.CtaOrdenante",
        }

        alias_objs = {}
        for field_alias, (rel_name, join_field) in special_fields.items():
            if field_alias in fields:
                rel = getattr(model, rel_name)
                alias_obj = aliased(rel.property.mapper.class_)
                alias_objs[field_alias] = alias_obj
                sql_query = sql_query.outerjoin(
                    alias_obj, getattr(model, join_field) == alias_obj.code
                )

        for field in fields:
            if field == "balance":
                column_balance = cls.get_balance_query_sql_with_cid(body, base_fields)
                sql_query = sql_query.add_column(column_balance.label("balance"))
                continue

            if field == "uuid_total_egresos_relacionados":
                uuid_origin_column, total_egresos_column, sql_query = (
                    cls.get_egresos_relacionados_query(model, sql_query, body)
                )
                sql_query = sql_query.add_column(
                    uuid_origin_column.label("CFDIs de egreso relacionados")
                )
                sql_query = sql_query.add_column(
                    total_egresos_column.label("Total egresos relacionados")
                )
                continue

            if field == "total_relacionados_single":
                uuid_origin_column, total_egresos_column, sql_query = (
                    cls.get_egresos_relacionados_query(model, sql_query, body)
                )
                sql_query = sql_query.add_column(
                    total_egresos_column.label("Total egresos relacionados")
                )
                continue

            base_field = getattr(model, field, None)
            if base_field:
                base_fields.append(base_field)
                sql_query = sql_query.add_column(base_field)
                continue

            if field in alias_objs:
                column = alias_objs[field].name
                sql_query = sql_query.add_column(column.label(field))
                if aggregate:
                    base_fields.append(column)
                continue

            relational_field, field_related = cls._get_relational_fields(cls.model, field)
            sql_query = cls._join_query_and_models_from_fields(sql_query, relational_field)
            field_related = field_related[0]

            if not aggregate or field in skip_aggregate_fields:
                sql_query = sql_query.add_column(field_related)
                if field in skip_aggregate_fields:
                    base_fields.append(field_related)
                continue

            is_numeric = field_related.type.python_type in (int, float, Decimal)
            if is_numeric:
                column = func.sum(field_related)
            else:
                if field == "paid_by.UUID":
                    column = func.string_agg(func.distinct(func.cast(field_related, String)), ", ")
                else:
                    column = func.string_agg(func.cast(field_related, String), ", ")

            sql_query = sql_query.add_column(column.label(field_related.name))

        if aggregate:
            sql_query = sql_query.group_by(*base_fields)

        return sql_query

    @classmethod
    def _get_relation_models_and_fields_from_domain(
        cls, model, domain
    ) -> list[tuple[Model, InstrumentedAttribute]]:
        relation_models_and_fields = []

        def process_condition(condition: DomainElement, current_model: type[Model]):
            """Procesa una condición individual [campo, operador, valor]"""
            if len(condition) != 3:
                return

            field_name = condition[0]
            _field, joins = cls._field_str_to_field_and_joins(current_model, field_name)
            relation_models_and_fields.extend(joins)

        def process_domain(domain_part):
            """Recorre el domain buscando condiciones"""
            if not isinstance(domain_part, list):
                return

            if len(domain_part) == 3 and isinstance(domain_part[0], str):
                process_condition(domain_part, model)
                return

            for item in domain_part:
                if isinstance(item, list):
                    process_domain(item)

        process_domain(domain)
        return relation_models_and_fields

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
        filters = get_filters(cls.model, domain, session)

        if filters is not None:
            query = query.filter(*filters)

        relation_models_and_fields = cls._get_relation_models_and_fields_from_domain(
            cls.model, domain
        )

        if fuzzy_search:
            query = cls._fuzzy_search(query, fuzzy_search)
        query = cls._join_query_and_models_from_fields(query, relation_models_and_fields)
        return query

    @classmethod
    def get_count(cls, query, has_payments=False) -> int:
        if has_payments:
            subq = query.order_by(None).subquery()
            return query.session.query(func.count()).select_from(subq).scalar() or 0
        else:
            return query.with_entities(func.count()).scalar() or 0

    @classmethod
    def _get_query_results(cls, query, *, session: Session):
        return query.all()

    @classmethod
    def _field_str_to_field_and_joins(
        cls, base_model: type[Model], field_str: str
    ) -> tuple[InstrumentedAttribute, list[tuple[Model, InstrumentedAttribute]]]:
        # "c_moneda.code" -> CatMoneda.code, CFDI.c_moneda
        join_fields: list[tuple[Model, InstrumentedAttribute]] = []
        current_model = base_model
        parts = field_str.split(".")
        has_to_many = False
        for part in parts[:-1]:  # All but the last part are relationships
            field = getattr(current_model, part, None)
            if field is None:
                raise NotFoundError(f"Column {part} not found in model {current_model.__name__}")

            if not hasattr(field, "property") and hasattr(field.property, "mapper"):
                raise NotFoundError(f"Expected relationship field, got {part}")
            current_model = get_alias_or_model(field)
            if field.property.uselist:
                has_to_many = True
            else:
                join_fields.append((current_model, field))
        final_field = getattr(current_model, parts[-1])
        if has_to_many:
            # Si hay to_many, no se hacen joins, para evitar afectar la cardinalidad
            join_fields = []
        return final_field, join_fields

    @staticmethod
    def _parse_fields_with_order(fields: FieldsLabeled) -> list[FieldInfo]:
        """Convierte dict a lista ordenada con metadatos"""
        return [
            FieldInfo(
                original_key=key,
                label=label,
                position=i,
                relation_path=key.split(".")[0] if "." in key else "",
                field_path=".".join(key.split(".")[1:]) if "." in key else key,
            )
            for i, (key, label) in enumerate(fields.items())
        ]

    @classmethod
    def _group_and_position_fields(
        cls, field_infos: list[FieldInfo], model: type[Model]
    ) -> list[ColumnSpec]:
        """Agrupa campos y genera ColumnSpecs manteniendo orden"""

        def find_to_many_path(field_path: str) -> str:
            """Encuentra la primera relación to_many en el path"""
            parts = field_path.split(".")
            current_model = model

            for i, part in enumerate(parts[:-1]):
                relation_attr = getattr(current_model, part)
                if relation_attr.property.uselist:
                    return ".".join(parts[: i + 1])
                current_model = get_alias_or_model(relation_attr)
            return ""

        # Agrupar campos por tipo
        to_many_groups = OrderedDict()
        other_fields = []
        first_positions = {}

        for field_info in field_infos:
            to_many_path = (
                find_to_many_path(field_info.original_key) if field_info.relation_path else ""
            )

            if to_many_path:
                # Es campo to_many
                if to_many_path not in first_positions:
                    first_positions[to_many_path] = field_info.position
                to_many_groups.setdefault(to_many_path, []).append(field_info)
            else:
                # Es campo directo o to_one
                other_fields.append(field_info)

        # Generar ColumnSpecs
        column_specs = []

        # Campos directos y to_one
        for field_info in other_fields:
            if field_info.relation_path:
                # to_one: usar field_str_to_field_and_joins
                final_field, _ = cls._field_str_to_field_and_joins(model, field_info.original_key)
            else:
                # directo: acceso simple
                final_field = getattr(model, field_info.field_path)

            column_specs.append(
                ColumnSpec(
                    column_expr=final_field,
                    label=field_info.label,
                    position=field_info.position,
                    is_subquery=False,
                )
            )

        # Campos to_many como subqueries
        for to_many_path, fields_in_group in to_many_groups.items():
            column_specs.append(
                ColumnSpec(
                    column_expr=cls._build_to_many_subquery(model, to_many_path, fields_in_group),
                    label=to_many_path,
                    position=first_positions[to_many_path],
                    is_subquery=True,
                )
            )

        return sorted(column_specs, key=lambda x: x.position)

    @classmethod
    def _build_to_many_subquery(
        cls, base_model: type[Model], to_many_path: str, field_infos: list[FieldInfo]
    ):
        """
        Construye subquery para cualquier relación to_many (simple o anidada)
        """
        # Navegar al modelo que contiene la relación to_many
        path_parts = to_many_path.split(".")
        current_model = base_model
        joins_before_to_many = []

        # Navegar hasta el modelo que contiene la relación to_many
        for part in path_parts[:-1]:
            relation_attr = getattr(current_model, part)
            joins_before_to_many.append((current_model, relation_attr))
            current_model = get_alias_or_model(relation_attr)

        # La última parte es la relación to_many
        to_many_relation = getattr(current_model, path_parts[-1])

        if to_many_relation.property.secondary is not None:
            # Si hay secondaryjoin, utilizamos el alias
            to_many_model = get_alias_or_model(to_many_relation)
        else:
            # Se usa el modelo directo, sin alias, para poderlo utilizar en el `where`
            # Si se utilizará el alias, tendríamos que modificar el where
            # sustituyendo el modelo por su alias
            to_many_model = to_many_relation.property.mapper.class_

        # Construir JSON con todos los campos
        # Estrategia para evitar productos cartesianos y permitir atributos de relaciones:
        # - Para cada parte intermedia de la ruta (relación), acumulamos el atributo
        #   de relación concreto para hacer outerjoin explícito y mantener el FROM anclado.
        # - Para la hoja (columna), navegamos usando la clase objetivo del último join
        #   (get_alias_or_model) de forma que `relacion.columna` sea válido para SQLAlchemy.
        #   Esto evita el error de Comparator sin atributo y reduce FROMs implícitos.
        json_pairs = []
        join_attrs: list[Any] = []  # relación ORM attributes en orden

        for field_info in sorted(field_infos, key=lambda x: x.position):
            # Obtener el path del campo relativo al modelo to_many
            full_path = field_info.original_key.split(".")
            field_path_in_to_many = full_path[len(path_parts) :]

            if not field_path_in_to_many:
                continue

            # Resolver expresión del campo acumulando relaciones y llegando a la clase destino
            current_left: Any = to_many_model
            for rel_name in field_path_in_to_many[:-1]:
                rel_attr = getattr(current_left, rel_name)
                # Alinear el alias del JOIN con el alias usado en la expresión final
                right_alias = get_alias_or_model(rel_attr)
                rel_attr = rel_attr.of_type(right_alias)
                join_attrs.append(rel_attr)
                current_left = right_alias

            # Obtener el último atributo (columna o propiedad híbrida) desde la clase destino
            leaf_attr = field_path_in_to_many[-1]
            final_expr = getattr(
                current_left if field_path_in_to_many[:-1] else to_many_model, leaf_attr
            )

            # Usar solo la parte relativa al modelo to_many como clave JSON
            json_key = ".".join(field_path_in_to_many)
            json_pairs.extend([json_key, final_expr])

        # Construir subquery anclada a la relación to_many
        # Evitamos FROMs implícitos seleccionando explícitamente el origen correcto.
        subquery = select(
            func.coalesce(
                func.jsonb_agg(func.jsonb_build_object(*json_pairs)), literal([], type_=JSONB)
            )
        )
        if to_many_relation.property.secondary is not None:
            # Relaciones con tabla secundaria: partir de la secundaria y unir al modelo destino
            subquery = subquery.select_from(to_many_relation.property.secondary).join(
                to_many_model, to_many_relation.property.secondaryjoin
            )
        else:
            # Relaciones directas: partir del modelo destino directamente
            subquery = subquery.select_from(to_many_model)

        # Añadir joins internos necesarios para campos dentro del to_many
        seen: set[str] = set()
        for rel_attr in join_attrs:
            key = str(rel_attr)
            if key in seen:
                continue
            seen.add(key)
            subquery = subquery.outerjoin(rel_attr)

        # Correlacionar apropiadamente contra el modelo padre usando el primaryjoin
        subquery = subquery.where(to_many_relation.property.primaryjoin).correlate(current_model)

        # El primaryjoin correlaciona contra el modelo padre; cuando hay
        # 'secondary', ya está presente en el FROM por el bloque anterior.

        return subquery.scalar_subquery()

    @classmethod
    def _get_query_model(cls, session: Session, fields: FieldsLabeled, domain: Domain = None):
        """
        Genera query SQLAlchemy con campos anidados y relaciones to_many como subqueries JSON
        Preserva el orden de los campos del input original.
        """
        # 1. Parsear campos preservando orden original
        field_infos = cls._parse_fields_with_order(fields)

        # 2. Agrupar y generar ColumnSpecs manteniendo posicionamiento
        column_specs = cls._group_and_position_fields(field_infos, cls.model)

        # 3. Construir query con columnas ordenadas
        select_columns = [spec.column_expr.label(spec.label) for spec in column_specs]
        # Asegurar que el FROM principal esté anclado al modelo base para evitar
        # decisiones implícitas de FROM que puedan generar productos cartesianos
        query: Query = session.query(*select_columns).select_from(cls.model)

        # 4. Añadir joins necesarios para campos to_one
        joins_needed = {}  # Usar dict con key única para deduplicar
        for spec in column_specs:
            if not spec.is_subquery:
                # Para campos to_one, necesitamos determinar los joins
                field_parts = [
                    info.original_key
                    for info in field_infos
                    if info.label == spec.label and info.relation_path
                ]
                for field_key in field_parts:
                    if "." in field_key:
                        _, join_fields = cls._field_str_to_field_and_joins(cls.model, field_key)
                        for join_model, join_field in join_fields:
                            # Usar el key del relationship como clave única
                            join_key = join_field.key
                            joins_needed[join_key] = (join_model, join_field)

        # 5. Aplicar joins únicos
        for join_model, join_field in joins_needed.values():
            query = cls._join_query_and_models_from_fields(query, [(join_model, join_field)])

        return query

    @classmethod
    def _get_search_query(
        cls,
        domain: Domain,
        fields: FieldsLabeled,
        order_by: str = "",
        limit: int | None = None,
        offset: int = 0,
        active: bool = True,
        fuzzy_search: str = "",
        *,
        session: Session,
        internal_domain: list[Any] | None = None,
    ) -> Query:
        return cls._get_search_query_and_count(
            domain,
            fields,
            order_by,
            limit,
            offset,
            active,
            fuzzy_search,
            need_count=False,
            session=session,
            internal_domain=internal_domain,
        )[0]

    @classmethod
    def apply_active_filter_if_needed(cls, query: Query, active_value: bool) -> None:
        if "active" not in cls.model.__table__.c:
            return query
        active_filter = cls.model.active == active_value
        active_filter = (
            or_(active_filter, cls.model.active is None) if active_value else active_filter
        )
        return query.filter(active_filter)

    @classmethod
    def _get_search_query_and_count(
        cls,
        domain: Domain,
        fields: FieldsLabeled,
        order_by: str = "",
        limit: int | None = None,
        offset: int = 0,
        active: bool = True,
        fuzzy_search: str = "",
        *,
        need_count: bool = False,
        session: Session,
        internal_domain: list[Any] | None = None,
    ) -> tuple[Query, int]:
        session.info = session.info or {}
        session.info["context"] = json.dumps(
            {
                "domain": domain,
                "limit": limit,
                "fuzzy_search": fuzzy_search,
                "order_by": order_by,
                "offset": offset,
                "active": active,
            },
            default=str,
        )

        if fuzzy_search:
            log(
                Modules.SEARCH,
                DEBUG,
                "FUZZY_SEARCH",
                {
                    "fuzzy_term": fuzzy_search,
                },
            )

        has_payments = any(filter[0] == "payments.FormaDePagoP" for filter in domain)

        query = cls._get_query_model(session=session, fields=fields, domain=domain)

        if has_payments:
            query = query.distinct(cls.model.UUID).order_by(cls.model.UUID)

        if internal_domain is not None:
            query = query.filter(internal_domain)
        query = cls.apply_domain(query, domain=domain, fuzzy_search=fuzzy_search, session=session)
        query = cls.apply_active_filter_if_needed(query, active)
        if not order_by:
            order_by = cls._get_default_order_by(session=session)
        if cls.default_distinct:
            query = query.distinct()
        count = -1
        if need_count:
            count = cls.get_count(query, has_payments)
        query = cls._apply_order_by(cls.model, order_by, query)
        if limit is not None:
            offset = offset or 0
            query = query.offset(offset * limit).limit(limit)
        return query, count

    @classmethod
    def _search(
        cls,
        domain: Domain,
        fields: list[str] | FieldsLabeled,
        order_by: str = "",
        limit: int | None = None,
        offset: int = 0,
        active: bool = True,
        fuzzy_search: str = "",
        *,
        need_count: bool = False,
        session: Session,
        lazzy: bool = False,
        internal_domain: list[Any] | None = None,
    ) -> Query | tuple[list[Model], int]:
        fields = ensure_fields_labeled(fields)
        query, count = cls._get_search_query_and_count(
            domain,
            fields,
            order_by,
            limit,
            offset,
            active,
            fuzzy_search or "",
            need_count=need_count,
            session=session,
            internal_domain=internal_domain,
        )
        if lazzy:
            return query
        records = (
            cls._get_query_results(query, session=session)
            if query.statement.selected_columns
            else []
        )  # TODO Make explicit which fields needed
        return (records, count) if need_count else records

    @classmethod
    def search(
        cls,
        domain: Domain,
        fields: list[str],
        order_by: str = "",
        limit: int | None = None,
        offset: int = 0,
        active: bool = True,
        fuzzy_search: str = "",
        *,
        internal_domain: list[Any] | None = None,
        session: Session,
        context=None,
    ) -> SearchResultPaged:
        next_page = False
        records, total_records = cls._search(
            domain,
            fields,
            order_by,
            limit,
            offset,
            active,
            fuzzy_search,
            session=session,
            need_count=True,
            internal_domain=internal_domain,
        )
        if limit and len(records) > limit:
            records.pop()
            next_page = True
        return records, next_page, total_records

    @classmethod
    def _check_data_key_value(cls, key, value):
        if key in cls.restricted_fields:
            raise ForbiddenError(f"The field '{key}' can not be set manually")

    @classmethod
    def _check_to_update_data(cls, data: dict[str, Any], *, session: Session, context=None):
        for key in data:
            if key in cls.restricted_update_fields:
                raise ForbiddenError(f"The field '{key}' can not be updated manually")

    @classmethod
    def _check_data(cls, record: Model, data: dict[str, Any], *, session: Session, context=None):
        is_active = getattr(record, "active", None)
        if is_active is None:
            is_active = True
        if not is_active and not data.get("active", False):
            raise ForbiddenError(
                f"The inactive records {cls.log_records(record)} cannot be updated"
            )
        if is_super_user(context):
            return
        for key, value in data.items():
            cls._check_data_key_value(key, value)

    @classmethod
    @check_context
    def create(cls, data: dict[str, Any], *, session: Session, context=None):
        data["identifier"] = data.get("identifier", identifier_default_factory())
        try:
            m2m = []
            for key, value in data.copy().items():
                if m2m_rel := is_x2m(cls.model, key):
                    data.pop(key)
                    m2m.append((m2m_rel, key, value))
                    continue
            record = cls.model(**data)
            for rel, key, value in m2m:
                field = getattr(record, key)
                cls._set_m2m(rel.property.entity, field, value, session=session)  # type: ignore
        except TypeError as e:
            raise ForbiddenError(e) from e
        cls._check_data(record, data, session=session, context=context)
        session.add(record)
        session.flush()
        return record

    @staticmethod
    def _to_primitive(data) -> PrimitiveType:
        def get_class(field):
            return enum.Enum if issubclass(field.__class__, enum.Enum) else field.__class__

        value_class = get_class(data)

        converters = {
            datetime: lambda x: x.isoformat(),
            date: lambda x: x.isoformat(),
            enum.Enum: lambda x: x.name,
            Identifier: lambda x: str(x),  # pylint: disable=unnecessary-lambda
        }
        return converters.get(value_class, lambda x: x)(data)

    @classmethod
    def record_to_dict(cls, record) -> dict[str, Any]:
        """Return a dictionary with the fields given (can use dot to indicate subfields)
        filled with the object data

        Args:
            record (Model): Record to convert

        Returns:
            Dict[str, Any]: Dictionary with the fields filled with the object data
        """
        plain_dict = dict(record)

        def tree():
            return defaultdict(tree)

        # Crear un defaultdict infinito
        res_dict = tree()
        for key, value in plain_dict.items():
            current_level = res_dict
            levels = key.split(".")
            if isinstance(value, list) and len(levels) == 1:
                # Manejar listas: solo procesar recursivamente si contienen objetos complejos (Modelos o Dicts)  # noqa: E501
                # Lo demás (primitivos de JSONB, datetimes, etc.) se pasa directo
                if value and (hasattr(value[0], "__table__") or isinstance(value[0], dict)):
                    current_level[levels[0]] = [cls.record_to_dict(item) for item in value]
                else:
                    current_level[levels[0]] = value
                continue
            for level in levels[:-1]:
                current_level = current_level[level]
            if isinstance(value, datetime):
                value = value.isoformat()
            current_level[levels[-1]] = value
        return res_dict

    @classmethod
    @ensure_list
    def to_nested_dict(
        cls,
        records: list[Model | TenantBaseModel],
        fields: set[str] = None,
        *,
        context=None,
    ) -> SearchResult:
        result = [
            json.loads(json.dumps(cls.record_to_dict(record), default=json_default))
            for record in records
        ]
        return result

    @classmethod
    def get_user_companies(cls, user: User, *, session: Session) -> set[Company]:
        session.add(user)
        permissions = session.query(Permission).filter(Permission.user_id == user.id).all()
        return {permission.company for permission in permissions}

    @classmethod
    @ensure_list
    def check_companies(cls, records: list[Model], *, session: Session, context=None):
        user = context["user"]
        session.add(user)
        allowed_companies = cls.get_user_companies(user, session=session)
        allowed_companies_ids = {company.id for company in allowed_companies}
        session.add_all(records)
        company_ids = cls.get_company_ids(records, session=session)
        if not_allowed_companies := company_ids - allowed_companies_ids:
            raise UnauthorizedError(f"Companies `{not_allowed_companies}` not allowed")

    @staticmethod
    def get_model_name_from_records(records: list[Model]) -> str:
        for record in records:
            return record.__class__.__name__
        return ""

    @classmethod
    @ensure_list
    def log_records(cls, records):
        if not records:
            return ""
        model_name = cls.get_model_name_from_records(records)
        return f"{model_name}({', '.join([str(r.id) for r in records])})"

    @classmethod
    @check_context
    @ensure_set
    def get(cls, ids: set[str], *, session: Session, context=None, singleton=True) -> list[Model]:
        log_in(ids)
        records = (
            session.query(cls.model)
            .filter(cls.model.identifier.in_(ids))
            .order_by(cls.model.identifier)
            .all()
        )
        if len(ids) != len(records):
            ids_read = {record.id for record in records}
            diff = ids - ids_read
            raise NotFoundError(f"ID's: {diff} were not found in model {cls.model.__name__}")
        if singleton and len(ids) == 1:
            return records[0]
        return records

    @classmethod
    def get_company_ids(cls, records: list[Model], *, session: Session) -> set[int]:
        session.add_all(records)
        company_ids = set()
        for record in records:
            if record.__table__.c.get("company_id") is not None:
                company_ids.add(record.company_id)
            else:
                raise UnauthorizedError(
                    f"The model {cls.model.__name__} have not function to get companies"
                )
        return company_ids

    @classmethod
    def _remove_from_m2m(cls, model, field, records: list[Model], *, session: Session):
        for record in records:
            if record in field:
                field.remove(record)
            else:
                raise NotFoundError(cls.log_records(record))

    @classmethod
    def _delete_m2m_rel(cls, model, field, records: list[Model], *, session: Session):
        for record in records:
            if record in field:
                session.delete(record)
            else:
                raise NotFoundError(cls.log_records(record))

    @classmethod
    def _add_to_m2m(cls, model, field, records: list[Model], *, session: Session):
        for record in records:
            if record in field:
                raise ForbiddenError(f"{cls.log_records(record)} already in relation")
        field.extend(records)

    @classmethod
    def _remove_all_from_m2m(cls, model, field, rel_id: int, *, session: Session):
        field.clear()

    @classmethod
    def _replace_all_from_m2m(cls, model, field, records: Model | list[Model], *, session: Session):
        field.clear()
        records = records if isinstance(records, list) else [records]
        field.extend(records)

    @classmethod
    def _set_m2m(
        cls,
        model,
        field,
        value: list[tuple[int, int | None | list[int]]],
        *,
        session: Session,
    ):
        """Update an m2m field based on the next structure:
        (0, None): NotImplemented
        (1, id): NotImplemented
        (2, id): Remove but NOT delete from the DB
        (3, id): Remove and delete from the DB
        (4, id): Add in relation
        (5, None): Remove all ids from the relation
        (6, ids): Replace all current ids with the provided ids

        Args:
            field ([type]): [description]
            value (List[Tuple[int, Union[int, None, List[int]]]]): [description]
            session ([type], optional): [description]. Defaults to None.
        """
        actions = {
            0: NotImplementedError,
            1: NotImplementedError,
            2: cls._remove_from_m2m,
            3: cls._delete_m2m_rel,
            4: cls._add_to_m2m,
            5: cls._remove_all_from_m2m,
            6: cls._replace_all_from_m2m,
        }
        for action, ids in value:
            if action not in actions or actions[action] is NotImplementedError:
                raise MethodNotAllowedError(f"Action {action} not implemented")
            ids = ids if isinstance(ids, list) else ids and [ids] or []
            records = [session.query(model).get(id) for id in ids]
            if None in records:
                raise NotFoundError(f"ID's: {ids} not found")
            actions[action](model, field, records, session=session)

    @classmethod
    @ensure_list
    @check_context
    def update(
        cls,
        records: list[Model],
        data: dict[str, Any],
        *,
        session: Session,
        context=None,
    ) -> list[Model]:
        session.add_all(records)
        for record in records:
            cls._check_data(record, data, session=session, context=context)
            cls._check_to_update_data(data, session=session, context=context)
            for key, value in data.items():
                if m2m_rel := is_x2m(record, key):
                    field = getattr(record, key)
                    cls._set_m2m(m2m_rel.property.entity, field, value, session=session)
                    continue
                setattr(record, key, value)
        return records

    @classmethod
    @ensure_list
    def delete(cls, records: list[Model], *, session: Session, context=None) -> set[int]:
        session.add_all(records)
        cls.check_companies(records, session=session, context=context)
        ids = [record.id for record in records]
        session.query(cls.model).filter(cls.model.id.in_(ids)).delete()
        return set(ids)

    @classmethod
    def get_owned_by(cls, user: User, *, session: Session, context=None) -> list[Company]:
        session.add(user)
        return session.query(Workspace).filter(Workspace.owner_id == user.id).all()

    @staticmethod
    def to_xlsx(query: Iterable[CFDI], fields: list[str], resume, session, context) -> bytes:
        wb = Workbook()
        ws = wb.active
        ws.title = "Cfdis"
        fields_names = []
        for field in fields:
            if field == "paid_by.UUID":
                fields_names.append("CFDIs De Pago Relacionados")
            elif field == "efos.state":
                fields_names.append("Estatus")
            else:
                fields_names.append(ColumnsNameExcel[field].value)
        ws.append(fields_names)
        for record in query:
            serializer = ModelSerializer(process_iterable=process_iterable)
            data = serializer.serialize(record, fields)
            ws.append(data)
        for column_cells in ws.columns:
            length = max(len(str(cell.value)) for cell in column_cells)
            ws.column_dimensions[column_cells[0].column_letter].width = length * 1.1  # Magic Number
        ws2 = wb.create_sheet("Totales")
        ws2.append(resume_fields)
        if resume["filtered"]:
            filtered = [
                "Periodo",
                resume["filtered"]["count"],
                resume["filtered"]["RetencionesIVA"],
                resume["filtered"]["RetencionesIEPS"],
                resume["filtered"]["RetencionesISR"],
                resume["filtered"]["TrasladosIVA"],
                resume["filtered"]["TrasladosIEPS"],
                resume["filtered"]["TrasladosISR"],
                resume["filtered"]["ImpuestosRetenidos"],
                resume["filtered"]["SubTotal"],
                resume["filtered"]["Descuento"],
                resume["filtered"]["Neto"],
                resume["filtered"]["Total"],
            ]

            ws2.append(filtered)

        if resume["excercise"]:
            excercise = [
                "Acumulado",
                resume["excercise"]["count"],
                resume["excercise"]["RetencionesIVA"],
                resume["excercise"]["RetencionesIEPS"],
                resume["excercise"]["RetencionesISR"],
                resume["excercise"]["TrasladosIVA"],
                resume["excercise"]["TrasladosIEPS"],
                resume["excercise"]["TrasladosISR"],
                resume["excercise"]["ImpuestosRetenidos"],
                resume["excercise"]["SubTotal"],
                resume["excercise"]["Descuento"],
                resume["excercise"]["Neto"],
                resume["excercise"]["Total"],
            ]

            ws2.append(excercise)

        for column_cells in ws2.columns:
            length = max(len(str(cell.value)) for cell in column_cells)
            ws2.column_dimensions[column_cells[0].column_letter].width = (
                length * 1.1
            )  # Magic Number

        with NamedTemporaryFile(suffix="xlsx") as f:
            wb.save(f.name)
            with open(f.name, "rb") as f2:
                return f2.read()

    @staticmethod
    def get_xml(records: list[Model]) -> list[dict[str, str]]: ...

    def to_xml(self, query: Query, _fields: list[str], session, context) -> bytes:
        """Return a ZIP with the XML's of the records"""
        cfdis = self.get_xml(query.all())
        f = io.BytesIO()
        with ZipFile(f, "w") as zf:
            for row in cfdis:
                uuid, xml_content = row["uuid"], row["xml_content"]
                if not xml_content:
                    log(
                        Modules.SEARCH,
                        WARNING,
                        "NO_XML_CONTENT",
                        {
                            "uuid": uuid,
                        },
                    )
                    continue
                zf.writestr(f"{uuid}.xml", xml_content)
        return f.getvalue()

    def to_pdf(
        self, query: Iterable[Model | TenantBaseModel], fields, session: Session, context
    ) -> bytes: ...

    @classmethod
    def export(
        cls,
        export_data: dict,
        query: Query,
        fields: list[str],
        export_str: str,
        resume_export=None,
        *,
        session,
        context,
        resume_type=None,
    ) -> dict[str, str]:
        export_format = ExportFormat[export_str]
        EXPORTERS = {
            ExportFormat.XLSX: cls.to_xlsx,
            ExportFormat.XML: cls.to_xml,
            ExportFormat.PDF: cls.to_pdf,
        }
        exporter = EXPORTERS.get(export_format)
        extension = {
            "CSV": "csv",
            "XLSX": "xlsx",
            "XML": "zip",
            "PDF": "zip",
        }[export_str]
        if not exporter:
            raise NotFoundError(f"Export format {export_format} not implemented")

        log(
            Modules.SEARCH,
            DEBUG,
            "EXPORT",
            {
                "export_format": export_format,
                "fields": fields,
                "resume_export": resume_export,
                "resume_type": resume_type,
            },
        )
        if not query.count():
            return {
                "url": "EMPTY",
            }
        data_bytes = None
        if export_str in {"XLSX", "xlsx"}:
            xlsx_exporter = XLSXExporter()
            data_bytes = xlsx_exporter.export(
                export_data, query, fields, resume_export, resume_type
            )
        else:
            data_bytes = exporter(cls, query, fields, session, context)
        filename = f"{export_data['file_name']}.{extension}"

        s3_client().upload_fileobj(  # TODO deal with collisions
            io.BytesIO(data_bytes),
            envars.S3_EXPORT,
            filename,
        )
        s3_url = s3_client().generate_presigned_url(
            "get_object",
            Params={
                "Bucket": envars.S3_EXPORT,
                "Key": filename,
            },
            ExpiresIn=EXPORT_EXPIRATION,
        )
        return {
            "url": s3_url,
        }

    @staticmethod
    def resume(
        domain: Domain,
        fuzzy_search: str = None,
        *,
        session: Session,
        context,
        fields: list[str] = None,
    ):
        raise MethodNotAllowedError("Resume not implemented")

    @classmethod
    def generic_xlsx_export(
        cls,
        company_session: Session,
        file_name: str,
        fields_labeled: FieldsLabeled,
        domain: Domain,
        order_by: str = "",
        limit: int | None = None,
        offset: int = 0,
        fuzzy_search: str = "",
    ) -> CfdiExport:
        export_query = cls._get_search_query(
            domain=domain,
            fields=fields_labeled,
            order_by=order_by,
            limit=limit,
            offset=offset,
            fuzzy_search=fuzzy_search,
            session=company_session,
        )
        export = export_xlsx.generic_xlsx_export(
            file_name=file_name,
            query=export_query,
        )
        company_session.add(export)
        return export

    def update_multiple(
        self,
        records: list[dict[str, Any]],
        session: Session,
        model_keys: dict[str, Any],
    ) -> None:
        cfdis_list_for_update = []

        primary_keys_in_model = self.model.__table__.primary_key.columns.keys()

        for record in records:
            data = model_keys.copy()

            for field in record:
                if field in self.restricted_update_fields:
                    raise ForbiddenError(f"The field '{field}' can not be updated manually")

            data.update(record)

            if not all(key in data for key in primary_keys_in_model):
                raise ForbiddenError(
                    f"The record must contain all primary keys: {primary_keys_in_model}"
                )

            cfdis_list_for_update.append(data)

        session.bulk_update_mappings(self.model, cfdis_list_for_update)

        session.commit()


def ensure_fields_labeled(fields: list[str] | FieldsLabeled | None) -> FieldsLabeled:
    if not fields:
        return {}
    if isinstance(fields, dict):
        return fields
    if isinstance(fields, list):
        return dict(zip(fields, fields, strict=True))
    raise TypeError("Fields must be a list or a dict")
