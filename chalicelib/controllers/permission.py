import enum
from uuid import uuid4

from sqlalchemy.orm import Session

from chalicelib.controllers.common import CommonController
from chalicelib.schema.models import Company, Permission, User

Role = Permission.RoleEnum


def _coerce_permission_role(value) -> Role:
    """Normalize ORM / driver values so ``abilities_by_role`` lookup always works.

    Some PostgreSQL + SQLAlchemy combinations return ``permission.role`` as plain
    ``str``; ``abilities_by_role`` keys are ``Role`` enums — ``dict.get(str)`` would miss.
    """
    if isinstance(value, Role):
        return value
    if isinstance(value, str):
        return Role[value.upper()]
    raise TypeError(f"Unexpected permission.role type: {type(value)!r}")


class Ability(enum.Enum):
    UploadCerts = enum.auto()
    SATSync = enum.auto()


class Module(enum.Enum):
    SATSync = "sat_sync"
    Payroll = "payroll"


class PermissionController(CommonController):
    model = Permission

    abilities_by_role = {  # TODO
        Role.OPERATOR: {
            Ability.UploadCerts,
            Ability.SATSync,
        },
        Role.PAYROLL: {
            Ability.UploadCerts,
            Ability.SATSync,
        },
    }

    modules_by_role = {
        Role.OPERATOR: {
            Module.SATSync,
        },
        Role.PAYROLL: {
            Module.SATSync,
            Module.Payroll,
        },
    }

    @classmethod
    def get_modules_available(cls, user: User, company: Company, *, session: Session) -> list[str]:
        session.add(user)
        session.add(company)
        roles = cls.get_roles(user, company, session=session)
        modules: set[str] = set()
        for role in roles:
            if modules_by_role := cls.modules_by_role.get(role):
                modules.update(m.name for m in modules_by_role)
        return list(modules)

    @classmethod
    def get_roles(cls, user: User, company: Company, *, session: Session) -> set[Role]:
        session.add(user)
        session.add(company)
        records = (
            session.query(Permission.role)
            .filter(Permission.user_id == user.id, Permission.company_id == company.id)
            .all()
        )
        return {_coerce_permission_role(record[0]) for record in records}

    @classmethod
    def get_abilities(cls, user: User, company: Company, *, session: Session) -> set[Ability]:
        session.add(user)
        session.add(company)
        roles = cls.get_roles(user, company, session=session)
        abilities = set()
        for role in roles:
            abilities.update(cls.abilities_by_role.get(role, set()))
        return abilities

    @staticmethod
    def create_owner_permission(owner_id: int, company_id: int, *, session):
        if PermissionController.permission_owner_exists(owner_id, company_id, session):
            return
        permission_payroll = Permission(
            user_id=owner_id, company_id=company_id, role=Role.PAYROLL, identifier=str(uuid4())
        )
        permission_operator = Permission(
            user_id=owner_id, company_id=company_id, role=Role.OPERATOR, identifier=str(uuid4())
        )
        session.add(permission_payroll)
        session.add(permission_operator)

    @staticmethod
    def permission_owner_exists(user_id: int, company_id: int, session):
        permission_payroll = (
            session.query(Permission)
            .filter(
                Permission.user_id == user_id,
                Permission.company_id == company_id,
                Permission.role == Role.PAYROLL,
            )
            .first()
        )
        permission_operator = (
            session.query(Permission)
            .filter(
                Permission.user_id == user_id,
                Permission.company_id == company_id,
                Permission.role == Role.OPERATOR,
            )
            .first()
        )
        return permission_payroll and permission_operator
