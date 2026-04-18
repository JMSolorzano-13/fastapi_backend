# pylint: disable=cyclic-import
from datetime import datetime, timedelta
from typing import Any

from chalice import ForbiddenError
from sqlalchemy.orm import Session

from chalicelib.controllers import disable_if_dev, ensure_list
from chalicelib.controllers.common import CommonController

# from chalicelib.controllers.odoo import OdooController
from chalicelib.controllers.odoo import OdooController
from chalicelib.logger import DEBUG, log
from chalicelib.modules import Modules
from chalicelib.new.config.infra import envars
from chalicelib.new.product.infra.product_repository_sa import ProductRepositorySA
from chalicelib.new.stripe.infra.stripe_account_creator import StripeAccountCreator
from chalicelib.new.stripe.infra.stripe_subscription_creator import (
    StripeSubscriptionCreator,
)
from chalicelib.new.utils.datetime import utc_now
from chalicelib.schema.models import User as UserORM
from chalicelib.schema.models import Workspace


class WorkspaceController(CommonController):
    model = Workspace

    restricted_fields = {
        "license",
    } | CommonController.restricted_fields

    @staticmethod
    def _get_valid_until(product_license: dict[str, Any]) -> datetime:
        return product_license.get("date_end", utc_now() - timedelta(days=1))

    @staticmethod
    def _get_add_permission(product_license: dict[str, Any]) -> bool:
        details = product_license.get("details", {})
        return details.get("add_enabled", False)

    @staticmethod
    def _get_stripe_status(product_license: dict[str, Any]) -> datetime:
        return product_license.get("stripe_status", "unknown")

    @staticmethod
    def default_license() -> dict[str, Any]:
        now: datetime = utc_now()
        return {
            "date_start": now.isoformat(),
            "date_end": (now + envars.DEFAULT_LICENSE_LIFETIME).isoformat(),
            "details": WorkspaceController.default_license_details(),
        }

    @staticmethod
    def update_license(
        workspace: Workspace, product_license: dict[str, Any], *, session, initial: bool = False
    ) -> None:
        old_license = workspace.license
        workspace.license = product_license
        workspace.valid_until = WorkspaceController._get_valid_until(workspace.license)
        workspace.add_permission = WorkspaceController._get_add_permission(workspace.license)
        workspace.stripe_status = WorkspaceController._get_stripe_status(workspace.license)
        log(
            Modules.LICENSE,
            DEBUG,
            "UPDATING_LICENSE",
            {
                "workspace": workspace.id,
                "license": old_license,
                "new_license": workspace.license,
            },
        )
        if not initial and old_license != workspace.license:  # User is probably paying
            subscription_creator = StripeSubscriptionCreator()
            product_repo = ProductRepositorySA(session=session)
            subscription_creator.change_started_subscription(
                user=workspace.owner,
                license=workspace.license,
                product_repo=product_repo,
            )
        if not initial and need_stripe(session, workspace):
            account_creator = StripeAccountCreator()
            account_creator.new_account(workspace.owner)
            subscription_creator = StripeSubscriptionCreator()
            product_repo = ProductRepositorySA(session=session)
            subscription_creator.new_sub_from_current_license(
                workspace.owner, workspace.license, product_repo
            )

    @staticmethod
    def init_license(workspace: Workspace, *, session) -> None:
        if envars.mock.STRIPE:
            default_license_mock = WorkspaceController.get_secret_default_license()
            WorkspaceController.update_license(
                workspace, default_license_mock, session=session, initial=True
            )
        else:
            product_license = WorkspaceController.default_license()
            WorkspaceController.update_license(
                workspace, product_license, session=session, initial=True
            )

    @staticmethod
    def get_secret_default_license() -> dict:
        return {  # TODO restructuracion obtener sin hardcodear
            "id": 1,
            "date_start": "2024-07-03",
            "date_end": "2027-07-03",
            "details": {
                "max_emails_enroll": 1,
                "max_companies": 1,
                "exceed_metadata_limit": False,
                "add_enabled": False,
                "products": [{"identifier": "prod_MjDE9ihnCFzJn7", "quantity": 1}],
            },
            "stripe_status": "active",
        }

    @classmethod
    def create(
        cls,
        data: dict[str, Any],
        *,
        session: Session,
        context=None,
    ):
        user: UserORM = context["user"]
        session.add(user)
        data["owner_id"] = user.id
        data.pop("license", None)
        workspace = super().create(data, session=session, context=context)
        cls.init_license(workspace, session=session)
        session.commit()
        if envars.mock.ODOO:
            return workspace

        if envars.NOTIFY_ODOO:
            OdooController.new_workspace(workspace, session=session)

        if envars.NOTIFY_STRIPE:  # TODO events
            stripe_account_creator = StripeAccountCreator()
            stripe_account_creator.new_account(user)
            stripe_subscription_creator = StripeSubscriptionCreator()
            stripe_subscription_creator.new_subscription(
                user, cancel_delta=envars.STRIPE_DEFAULT_CANCEL_AT_DELTA
            )
        return workspace

    @classmethod
    @ensure_list
    def check_companies(cls, records: list[Workspace], *, session: Session, context=None):
        return True

    @classmethod
    def get_license_dict(cls, workspace: Workspace) -> dict[str, Any]:
        return workspace.license

    @staticmethod
    def default_license_details() -> dict[str, Any]:
        return {
            "max_companies": 1,
            "max_emails_enroll": 1,
        }

    @classmethod
    def license_attrib(cls, attrib: str, workspace: Workspace) -> Any:
        product_license = cls.get_license_dict(workspace)
        license_details = product_license.get("details", {})
        defaults = cls.default_license_details()
        return license_details.get(attrib, defaults[attrib])

    @classmethod
    def user_is_owner_or_invited(cls, user: UserORM, workspace: Workspace) -> bool:
        """
        Verifica si un usuario puede operar en un workspace.
        Un usuario puede operar si cumple alguna de estas condiciones:
        1. Es el owner del workspace (workspace.owner_id == user.id)
        2. Fue invitado por el owner del workspace (user.invited_by_id == workspace.owner_id)
        Args:
            user: Usuario a validar
            workspace: Workspace donde se quiere operar
        Returns:
            bool: True si puede operar, False en caso contrario
        Note:
            Esta validación es crítica para:
            - Creación de empresas
            - Asignación de permisos
            - Operaciones multi-tenant
        """
        # Condición 1: Usuario es el owner del workspace
        is_owner = workspace.owner_id == user.id

        # Condición 2: Usuario fue invitado por el owner del workspace
        is_invited_by_workspace_owner = user.invited_by_id == workspace.owner_id

        return is_owner or is_invited_by_workspace_owner

    @classmethod
    @disable_if_dev
    def check_can_create_companies(
        cls, workspace: Workspace, *, session: Session, context=None
    ) -> None:
        """Ensure the workspace license can create more companies"""
        from chalicelib.controllers.user import (  # pylint: disable=import-outside-toplevel
            UserController,
        )

        user = context["user"]
        session.add(user)
        session.add(workspace)

        if not workspace.companies:
            return

        if not workspace.is_active:
            raise ForbiddenError(f"{cls.log_records(workspace)} is not active")

        if UserController.is_super_admin(user):
            return
        actual_companies = workspace.companies
        max_companies = cls.license_attrib("max_companies", workspace)
        if max_companies == "unlimited":
            return
        if len(actual_companies) >= max_companies:
            raise ForbiddenError(
                f"You have reached the maximum number of companies ({max_companies})"
                f" allowed in {cls.log_records(workspace)}"
            )

    @classmethod
    def get_by_user_id(cls, user_id: int, *, session: Session, context=None):
        from chalicelib.controllers.user import (  # pylint: disable=import-outside-toplevel
            UserController,
        )

        workspace = session.query(Workspace).filter(Workspace.owner_id == user_id).first()
        if not workspace:
            user = UserController.get(user_id, singleton=True, session=session, context=context)
            context = {"user": user}
            workspace = cls.create({"name": user.email}, session=session, context=context)
        return workspace

    @classmethod
    def set_licenses(cls, licenses: list[dict[str, Any]], *, session: Session, context=None):
        """Set the licenses for the workspaces"""
        from chalicelib.controllers.user import (  # pylint: disable=import-outside-toplevel
            UserController,
        )

        user = context["user"]
        session.add(user)
        UserController.ensure_external_super_user(user, "set the license", session=session)
        for product_license in licenses:
            if "user_email" in product_license:
                workspace = (
                    session.query(Workspace)
                    .join(Workspace.owner)
                    .filter(UserORM.email == product_license.pop("user_email"))
                    .one_or_none()
                )
                if not workspace:
                    raise ValueError(
                        f"No workspace found for user with email {product_license['user_email']}"
                    )
            else:
                user_id = product_license.pop("user_id")
                workspace = cls.get_by_user_id(user_id, session=session, context=context)
            WorkspaceController.update_license(workspace, product_license, session=session)


def need_stripe(session: Session, workspace: Workspace) -> bool:
    """Check if the workspace needs a stripe account"""
    user = session.query(UserORM).filter(UserORM.id == workspace.owner_id).one()
    return not bool(user.stripe_subscription_identifier)
