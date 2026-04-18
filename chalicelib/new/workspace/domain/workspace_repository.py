from typing import Protocol

from chalicelib.new.shared.domain.primitives import Identifier
from chalicelib.new.workspace.domain.workspace import Workspace


class WorkspaceRepository(Protocol):
    def save(self, company: Workspace):
        raise NotImplementedError

    def get_by_identifier(self, identifier: Identifier) -> Workspace:
        raise NotImplementedError
