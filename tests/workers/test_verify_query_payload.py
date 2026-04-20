"""Tests for verify queue JSON normalization (RFC ambiguity, aliases)."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

from chalicelib.workers.verify_query_payload import normalize_verify_query_body


def test_normalize_ambiguous_rfc_resolves_via_sat_query_schema() -> None:
    """Two companies same RFC: pick the tenant whose ``sat_query`` row matches query ``identifier``."""
    body = (
        '{"identifier":"e7f58c77-73f4-40e5-8a2e-f52c344b6c86",'
        '"rfc":"SIE200729UA0","request_id":"209a17b8-8819-433a-a8c8-456010a9f623",'
        '"request_type":"CFDI","download_type":"ISSUED","technology":"WebService"}'
    )
    session = MagicMock()

    c_a = MagicMock()
    c_a.identifier = "00000000-0000-0000-0000-0000000000aa"
    c_a.workspace_id = 1
    c_a.id = 10
    c_a.rfc = "SIE200729UA0"

    c_b = MagicMock()
    c_b.identifier = "0f4d7bb3-0c1a-4a4c-abac-7d6f5a8404bf"
    c_b.workspace_id = 2
    c_b.id = 26
    c_b.rfc = "SIE200729UA0"

    qm = MagicMock()
    qm.filter.return_value = qm
    qm.all.return_value = [c_a, c_b]
    session.query.return_value = qm

    def exec_side_effect(stmt, params=None):
        s = str(stmt)
        if "00000000-0000-0000-0000-0000000000aa" in s:
            return MagicMock(fetchone=MagicMock(return_value=None))
        if "0f4d7bb3-0c1a-4a4c-abac-7d6f5a8404bf" in s:
            return MagicMock(fetchone=MagicMock(return_value=(1,)))
        return MagicMock(fetchone=MagicMock(return_value=None))

    session.execute.side_effect = exec_side_effect

    out = json.loads(normalize_verify_query_body(body, session))
    assert out["company_identifier"] == "0f4d7bb3-0c1a-4a4c-abac-7d6f5a8404bf"
    assert out["cid"] == 26
    assert out["wid"] == 2
