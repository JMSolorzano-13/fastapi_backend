from typing import Protocol

from chalicelib.new.company.domain import Company
from chalicelib.new.shared.domain.primitives import Identifier


class CompanyRepository(Protocol):
    def save(self, company: Company):
        raise NotImplementedError

    def get_by_identifier(self, identifier: Identifier) -> Company:
        raise NotImplementedError
