import functools
import inspect
import operator
import typing
from collections.abc import Callable
from contextlib import contextmanager
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, TypeVar, Union

from chalice import BadRequestError
from psycopg2 import errors
from sqlalchemy import and_, event, or_, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DatabaseError, OperationalError
from sqlalchemy.orm import Session
from sqlalchemy.sql.elements import BooleanClauseList, ColumnElement

from chalicelib.logger import ERROR, EXCEPTION, INFO, WARNING, log, log_in
from chalicelib.modules import Modules
from chalicelib.new.config.infra import envars
from chalicelib.new.utils.session import with_session
from chalicelib.schema import get_engine
from chalicelib.schema.models.model import Model

DomainElement = tuple[str, str, Any]
Operand = Any
Filter = Any
Domain = list[DomainElement | Operand]
SearchResult = list[dict[str, Any]]
SearchResultPaged = tuple[SearchResult, bool, int]


def text_like(column: ColumnElement, value: str):
    value = value.replace("%", "%%")  # Escape % characters
    value = f"%{value}%"  # Surround with % for LIKE matching
    # value = func.unaccent(value)  # Remove accents from the value # TODO
    return column.ilike(value)  # Case-insensitive LIKE comparison


operators = {
    "<": operator.lt,
    "<=": operator.le,
    "=": operator.eq,
    "!=": operator.ne,
    ">=": operator.ge,
    ">": operator.gt,
    "not": operator.not_,
    "is": operator.is_,
    "is not": operator.is_not,
    "like": text_like,
}


def _get_x2m_cardinal_filter(column, op, value):
    if value != "any":
        raise BadRequestError("Only value 'any' is accepted in relations")
    if op not in ("=", "!="):
        raise BadRequestError("Only the operators '=' and '!=' are accepted in relations")

    any_arg = None

    res = column.any(any_arg)

    if op == "!=":
        res = ~res
    return res


def _get_x2m_relational_filter(column, op, value):
    if not isinstance(value, list):
        raise BadRequestError(f"Invalid value for m2m field {column}, must be a list")
    if op not in ("in", "not in"):
        raise BadRequestError(f"Invalid operator {op} for m2m field")
    rel_model = get_alias_or_model(column)
    log_in(value)
    any_arg = rel_model.id.in_(value)

    res = column.any(any_arg)
    if op == "not in":
        res = ~res
    return res


def _get_filter_x2m(column, op, value):
    if op in ("=", "!="):
        return _get_x2m_cardinal_filter(column, op, value)

    return _get_x2m_relational_filter(column, op, value)


def is_m2o(column):
    if not hasattr(column, "property"):
        return False
    return hasattr(column.property, "mapper") and not column.property.uselist


def is_x2m(model: type[Model], field: str) -> Any:
    if not hasattr(model, "_sa_class_manager"):
        return False
    rel = model._sa_class_manager.get(field)  # pylint: disable=protected-access
    return rel and getattr(rel.property, "uselist", None) and rel


def _get_filter_m2o(column, op, value, session):
    if value != "any":
        raise BadRequestError("Only value 'any' is accepted in relations")
    column_rel = tuple(column.property.primaryjoin.left.base_columns)[0]
    column_rel_right = tuple(column.property.primaryjoin.right.base_columns)[0]
    expr = column_rel_right.in_(session.query(column_rel))
    if op == "=":
        return expr
    if op == "!=":
        return ~expr
    raise BadRequestError("Only the operators '=' and '!=' are accepted in relations")


def is_doted(de: DomainElement) -> bool:
    return de[0].find(".") != -1


def get_filter(model, de: DomainElement, session: Session):
    if is_doted(de):
        return _get_filter_doted(model, de, session)
    return _get_filter(model, de, session)


def get_filters(model: any, domain: Domain, session) -> list[Filter]:
    def is_condition(item: Domain):
        return (
            isinstance(item, (list | tuple))
            and len(item) == 3
            and isinstance(item[0], str)
            and isinstance(item[1], str)
        )

    def process_condition(condition: list | tuple) -> BooleanClauseList:
        return get_filter(model, condition, session)

    def process_and_conditions(conditions: list) -> BooleanClauseList | None:
        processed = []
        for cond in conditions:
            if is_condition(cond):
                processed.append(process_condition(cond))
            else:
                sub_result = get_filters(model, cond, session)
                if sub_result is not None:
                    processed.append(sub_result)

        if len(processed) == 1:
            return processed
        return and_(*processed) if processed else None

    def process_or_group(conditions: list) -> BooleanClauseList | None:
        processed = []
        for cond in conditions:
            if is_condition(cond):
                processed.append(process_condition(cond))
            elif isinstance(cond, list):
                sub_result = process_and_conditions(cond)
                if sub_result is not None:
                    processed.append(sub_result)
        return or_(*processed) if processed else None

    if not domain:
        return None

    if is_condition(domain):
        return process_condition(domain)

    if not isinstance(domain, list):
        raise ValueError(f"Domain debe ser una lista: {domain}")

    if domain and domain[0] == "|":
        if len(domain) != 2 or not isinstance(domain[1], list):
            raise ValueError("Grupo OR debe tener la forma ['|', [conditions]]")
        return process_or_group(domain[1])

    return process_and_conditions(domain)


def get_alias_or_model(attr):
    return getattr(attr, "alias", attr.property.mapper.class_)


def _get_filter_doted(model, domain: DomainElement, session):
    key, op, value = domain
    tokens = key.split(".")
    relations, field = tokens[:-1], tokens[-1]
    current_model = model
    filter_chain = []
    has_to_many = False
    for rel in relations:
        attrib = getattr(current_model, rel)
        current_model = get_alias_or_model(attrib)
        if attrib.property.uselist:
            filter_chain.append(attrib.any)
            has_to_many = True
        else:
            filter_chain.append(attrib.has)
    deepest_filter = _get_filter(current_model, (field, op, value), session)
    if has_to_many:
        for f in reversed(filter_chain):
            deepest_filter = f(deepest_filter)
    return deepest_filter


def _get_filter(model, de: DomainElement, session: Session):
    key, op, value = de
    column = getattr(model, key)
    if inspect.ismethod(column):
        return column() if value else ~column()
    if is_m2o(column):
        return _get_filter_m2o(column, op, value, session)
    if is_x2m(model, key):
        return _get_filter_x2m(column, op, value)

    if op == "in":
        log_in(value)
        return column.in_(value)
    if op == "not in":
        log_in(value)
        return ~column.in_(value)
    if value == "null":
        value = None
    real_op = operators[op]
    return real_op(column, value)


# def get_filters(model, domain: Domain, session) -> list[Filter]:
#     company_identifier = get_company_identifier(domain)
#     return [get_filter(model, dt, session, company_identifier) for dt in domain]


def get_company_identifier(domain: Domain) -> str:
    if not domain or not isinstance(domain[0], list) or domain[0][0] != "company_identifier":
        return None
    return domain[0][2] if domain[0][1] == "=" else None


def ensure_list(f):
    @functools.wraps(f)
    def wrapper(cls, records, *args, **kwargs):
        if records is None:
            records = []
        elif not isinstance(records, list):
            records = [records]
        return f(cls, records, *args, **kwargs)

    return wrapper


def ensure_set(f):
    @functools.wraps(f)
    def wrapper(cls, ids, *args, **kwargs):
        if not isinstance(ids, set):
            ids = {*ids} if isinstance(ids, list) else {ids}
        return f(cls, ids, *args, **kwargs)

    return wrapper


def ensure_dict_by_ids(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        dict_by_ids = args[1]
        if not isinstance(dict_by_ids, dict):
            raise BadRequestError("Invalid dict_by_ids")
        dict_by_ids = {int(k): v for k, v in dict_by_ids.items()}
        args = args[:1] + (dict_by_ids,) + args[2:]
        return f(*args, **kwargs)

    return wrapper


def is_super_user(context: dict[str, Any]):
    return "super_user" in context  # TODO user another technique


def scale_to_super_user(context: dict[str, Any] | None = None) -> dict[str, Any]:
    if context is None:
        context = {}
    context["super_user"] = True
    return context


def remove_super_user(context: dict[str, Any] = None):
    if context is None:
        context = {}
    context.pop("super_user", None)
    return context


def disable_if_dev(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        DEV_MODE = bool(envars.DEV_MODE)
        return None if DEV_MODE else f(*args, **kwargs)

    return wrapper
