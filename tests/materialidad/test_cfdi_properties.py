from pydantic import HttpUrl
from sqlalchemy.orm import Session

from chalicelib.controllers.cfdi import CFDIController
from chalicelib.schema.models.company import Company
from chalicelib.schema.models.tenant.cfdi import CFDI


def test_cfdi_properties(
    company_session: Session,
    cfdi: CFDI,
    attachments: dict[str, HttpUrl],
):
    assert len(cfdi.attachments) == len(attachments)
    assert cfdi.attachments_count == len(attachments)
    assert cfdi.attachments_size == sum(attachment.size for attachment in cfdi.attachments)
    cfdi2 = CFDI.demo()
    company_session.add(cfdi2)
    company_session.commit()
    assert cfdi2.attachments_count == 0
    assert cfdi2.attachments_size == 0
    assert cfdi2.attachments == []


def test_search(
    company_session: Session,
    cfdi: CFDI,
    company: Company,
    attachments: dict[str, HttpUrl],
):
    assert len(attachments) > 0
    assert sum(attachment.size for attachment in cfdi.attachments) > 0

    cfdi2 = CFDI.demo()
    company_session.add(cfdi2)
    company_session.commit()

    records, next_page, total_records = CFDIController.search(
        domain=[],
        fields=[
            "UUID",
            "attachments_count",
            "attachments_size",
        ],
        session=company_session,
    )
    dict_repr = CFDIController.to_nested_dict(records)
    assert total_records >= 2
    for record in dict_repr:
        if record["UUID"] == cfdi.UUID:
            assert record["attachments_count"] == len(attachments)
            assert record["attachments_size"] == sum(
                attachment.size for attachment in cfdi.attachments
            )
        elif record["UUID"] == cfdi2.UUID:
            assert record["attachments_count"] == 0
            assert record["attachments_size"] == 0
