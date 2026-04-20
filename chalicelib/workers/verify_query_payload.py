"""Normalize SAT verify-queue JSON before :class:`Query` validation.

Manual enqueue, legacy shapes, or reduced exports sometimes omit ``company_identifier``
and use ``rfc`` / ``request_id`` instead. The worker used to log ``PARSING_FAILED``,
skip handling, and still **complete** the Service Bus message—leaving ``sat_query``
stuck in ``SENT`` while verify messages were drained.
"""

from __future__ import annotations

import json
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from chalicelib.schema.models.company import Company


def _apply_aliases(data: dict[str, Any]) -> None:
    if data.get("query_identifier") and not data.get("identifier"):
        data["identifier"] = data["query_identifier"]
    if data.get("request_id") and not data.get("name"):
        data["name"] = data["request_id"]
    rfc = data.get("rfc")
    if rfc and not data.get("company_rfc"):
        data["company_rfc"] = rfc


def _company_owning_sat_query(session: Session, company_rows: list[Company], query_uuid: str) -> Company | None:
    """When several ``Company`` rows share the same RFC, pick the tenant whose ``sat_query`` has this query UUID."""
    if not query_uuid or not company_rows:
        return None
    qid = str(query_uuid).strip()
    for c in company_rows:
        schema = str(c.identifier)
        row = session.execute(
            text(
                f'SELECT 1 FROM "{schema}".sat_query '
                "WHERE identifier = CAST(:qid AS uuid) LIMIT 1"
            ),
            {"qid": qid},
        ).fetchone()
        if row:
            return c
    return None


def _load_company_row(session: Session, data: dict[str, Any]) -> Company | None:
    cid_raw = data.get("cid")
    wid_raw = data.get("wid")
    rfc = (data.get("company_rfc") or data.get("rfc") or "").strip()
    query_uuid = data.get("identifier")

    if data.get("company_identifier"):
        q = session.query(Company).filter(
            Company.identifier == str(data["company_identifier"]).strip()
        )
        return q.one_or_none()

    if cid_raw is not None:
        q = session.query(Company).filter(Company.id == int(cid_raw))
        if wid_raw is not None:
            q = q.filter(Company.workspace_id == int(wid_raw))
        return q.one_or_none()

    if rfc:
        rows = session.query(Company).filter(Company.rfc == rfc).all()
        if len(rows) == 1:
            return rows[0]
        if len(rows) > 1:
            return _company_owning_sat_query(session, rows, str(query_uuid) if query_uuid else "")
    return None


def _enrich_from_sat_query(session: Session, company: Company, data: dict[str, Any]) -> None:
    qid = data.get("identifier")
    if not qid or data.get("sent_date"):
        return
    schema = str(company.identifier)
    session.execute(text(f'SET LOCAL search_path TO "{schema}"'))
    try:
        row = session.execute(
            text(
                "SELECT sent_date, name FROM sat_query "
                "WHERE identifier = CAST(:qid AS uuid) LIMIT 1"
            ),
            {"qid": str(qid)},
        ).fetchone()
    finally:
        session.execute(text('SET LOCAL search_path TO "public"'))

    if not row:
        return
    sent_date, name = row[0], row[1]
    if sent_date is not None and not data.get("sent_date"):
        data["sent_date"] = sent_date.isoformat() if hasattr(sent_date, "isoformat") else str(sent_date)
    if name and not data.get("name"):
        data["name"] = name


def normalize_verify_query_body(body: str, session: Session) -> str:
    """Return JSON text acceptable by :class:`chalicelib.new.query.domain.query.Query`."""
    data = json.loads(body)
    if not isinstance(data, dict):
        return body
    _apply_aliases(data)
    company = _load_company_row(session, data)
    if company is not None:
        data["company_identifier"] = str(company.identifier)
        if data.get("wid") is None and company.workspace_id is not None:
            data["wid"] = int(company.workspace_id)
        if data.get("cid") is None:
            data["cid"] = int(company.id)
        if not (data.get("company_rfc") or data.get("rfc")) and company.rfc:
            data["company_rfc"] = company.rfc
        _enrich_from_sat_query(session, company, data)
    return json.dumps(data, default=str)
