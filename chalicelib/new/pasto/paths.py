import uuid
from dataclasses import dataclass

from chalicelib.new.shared.domain.primitives import Identifier


@dataclass
class MetadataPath:
    company_identifier: Identifier

    @property
    def path(self):
        return f"metadata/{self.company_identifier}.csv"


@dataclass
class XMLZipPath:
    company_identifier: Identifier

    @property
    def path(self):
        u = str(uuid.uuid4())
        return f"xml/{self.company_identifier}_{u}.zip"


@dataclass
class CancelPath:
    company_identifier: Identifier

    @property
    def path(self):
        u = str(uuid.uuid4())
        return f"cancel/{self.company_identifier}_{u}.csv"
