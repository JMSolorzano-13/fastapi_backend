from typing import Protocol

from chalicelib.new.fiel.domain.fiel import FIEL
from chalicelib.new.shared.domain.primitives import Identifier


class FielRepository(Protocol):
    def get_from_company_identifier(self, company_identifier: Identifier) -> FIEL:
        raise NotImplementedError

    def mark_as_not_valid_certs(self, company_identifier: Identifier):
        raise NotImplementedError
