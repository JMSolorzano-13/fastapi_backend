import pytest
from sqlalchemy.orm import Session

from chalicelib.new.license.infra.siigo_marketing import BASE_PRODUCT_ENABLE, MARKETING_EMAILS_KEY
from chalicelib.schema.models.workspace import Workspace


@pytest.fixture
def workspace_with_base_product(session: Session, workspace: Workspace) -> Workspace:
    workspace.license[BASE_PRODUCT_ENABLE] = True
    workspace.license[MARKETING_EMAILS_KEY] = {}
    return workspace
