from dataclasses import dataclass

from chalicelib.new.shared.domain.exceptions import DomainException
from chalicelib.new.shared.domain.primitives import Identifier


@dataclass
class CertsNotFound(DomainException):
    company_identifier: Identifier
