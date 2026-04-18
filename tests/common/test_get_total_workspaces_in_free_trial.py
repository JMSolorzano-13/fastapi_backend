from sqlalchemy.orm import Session

from chalicelib.new.license.infra.siigo_marketing import get_total_workspaces_in_free_trial
from chalicelib.schema.models.workspace import Workspace


def test_get_total_workspaces_in_free_trial(session: Session):
    """Test to verify 'get_total_workspaces_in_free_trial' function works correctly (returns an int)"""
    session.add_all([Workspace() for _ in range(100)])
    value = get_total_workspaces_in_free_trial(session)

    assert type(value) is int
    assert value == 100
