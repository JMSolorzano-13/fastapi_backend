from dataclasses import dataclass
from typing import Optional

from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from chalicelib.new.shared.domain.aggregation_root import AggregationRoot
from chalicelib.new.shared.domain.primitives import Identifier, identifier_default_factory
from chalicelib.schema.models.company import Company


@dataclass
class DomainEvent(AggregationRoot):
    @classmethod
    def from_event(cls, event: Optional["DomainEvent"]):
        if not event:
            raise ZeroDivisionError  # TODO: raise a proper exception
        event_dict = event.to_dict()
        identifier = event_dict.pop("identifier")
        return cls(**event_dict).set_identifier(identifier)


@dataclass
class CompanyWithSession(DomainEvent):
    company: Company
    company_session: Session


class CompanyEvent(BaseModel, DomainEvent):
    identifier: Identifier = Field(default_factory=identifier_default_factory, init=False)
    company_identifier: Identifier
    company_rfc: str = ""
