from typing import Protocol

from chalicelib.schema.models import User as UserORM
from chalicelib.schema.models import Workspace as WorkspaceORM

from .license_details import LicenseDetails


class LicenseRepository(Protocol):
    def get_current_used_characteristics(self, user: UserORM) -> LicenseDetails: ...

    def user_has_permission_to_modify(self, user: UserORM, workspace: WorkspaceORM) -> bool: ...
