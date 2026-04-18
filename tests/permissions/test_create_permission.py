from chalicelib.schema.models import Permission
from tests.fixtures.factories.permission import PermissionFactory


def test_create_permission():
    permission = PermissionFactory.build(role=Permission.RoleEnum.OPERATOR)

    assert permission.role == Permission.RoleEnum.OPERATOR
