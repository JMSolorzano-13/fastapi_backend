from dataclasses import dataclass
from typing import Any

from chalicelib.new.config.infra import envars
from chalicelib.new.product.domain import ProductRepository
from chalicelib.new.shared.domain.primitives import Identifier
from chalicelib.new.stripe.infra.stripe_subscription_updater import (
    InvoiceURL,
    StripeSubscriptionUpdater,
)
from chalicelib.new.workspace.infra import WorkspaceRepositorySA
from chalicelib.schema.models import User as UserORM
from chalicelib.schema.models import Workspace as WorkspaceORM

from .license_details import LicenseDetails
from .license_repository import LicenseRepository
from .product_line import ProductLine


class LicenseUpdateException(Exception):
    pass


@dataclass
class NotLicenseUpdateAllowed(LicenseUpdateException):
    user: UserORM
    workspace: WorkspaceORM

    def __str__(self) -> str:
        return f"User not allowed to update license: user={self.user.identifier}, workspace={self.workspace.identifier}"  # noqa E501


@dataclass
class UsersExceedNewLimit(LicenseUpdateException):
    current: int
    intended: int

    def __str__(self) -> str:
        return f"Users exceed new limit: {self.current} > {self.intended}"


@dataclass
class LicenseRequiresAction(LicenseUpdateException):
    license_url: str

    def __str__(self) -> str:
        return (
            f"The license requires a previous action, refers to the next link: {self.license_url}"
        )


@dataclass
class InvoicePending(LicenseUpdateException):
    invoice_id: str

    def __str__(self) -> str:
        return "The invoice is still pending"


@dataclass
class CompaniesExceedNewLimit(LicenseUpdateException):
    current: int
    intended: int

    def __str__(self) -> str:
        return f"Companies exceed new limit: {self.current} > {self.intended}"


@dataclass
class NoSinglePackageProvided(LicenseUpdateException):
    def __str__(self) -> str:
        return "One package must be provided"


@dataclass
class NegativeQuantity(LicenseUpdateException):
    def __str__(self) -> str:
        return "Quantity must be positive"


@dataclass
class DuplicatedProduct(LicenseUpdateException):
    def __str__(self) -> str:
        return "Duplicated product"


@dataclass
class LicenseSetter:
    license_repo: LicenseRepository
    workspace_repo: WorkspaceRepositorySA
    product_repo: ProductRepository
    stripe_updater: StripeSubscriptionUpdater

    def process_mock_license(self, workspace_identifier, license_details):
        get_license_current = (
            self.workspace_repo.session.query(WorkspaceORM.license).filter(
                WorkspaceORM.identifier == workspace_identifier
            )
        ).scalar()

        PACKAGE_IDENTIFIER = [
            "prod_MjDE9ihnCFzJn7",
            "prod_MO0U9eOMqHltxj",
            "prod_MO0VU7LytUw1Jg",
            "prod_MO0VPm6HZsxsnB",
            "prod_MO0WIdwwo2xI7U",
        ]
        for product_new in license_details["products"]:
            if product_new["identifier"] in ["prod_NNha17BGU5gqFC", "prod_MO0VF2GRtuuHpw"]:
                for product_current in list(get_license_current["details"]["products"]):
                    if product_current["identifier"] == "prod_MO0VF2GRtuuHpw":
                        product_current["quantity"] += product_new["quantity"]
                    else:
                        get_license_current["details"]["add_enabled"] = True
                        get_license_current["details"]["products"].append(product_new)
            else:
                if product_new["identifier"] in PACKAGE_IDENTIFIER:
                    for product_current in list(get_license_current["details"]["products"]):
                        if product_current["identifier"] in PACKAGE_IDENTIFIER:
                            product_current["identifier"] = product_new["identifier"]

        self.workspace_repo.session.query(WorkspaceORM).filter(
            WorkspaceORM.identifier == workspace_identifier
        ).update(
            {
                "license": get_license_current,
                "stripe_status": "active",
                "add_permission": True,
            }
        )
        self.workspace_repo.session.commit()

        return {"", 200}

    def try_set_license_detail(
        self,
        user: UserORM,
        workspace_identifier: Identifier,
        license_details: dict[str, Any],
        proration_date: int = None,
    ) -> InvoiceURL:
        if envars.mock.STRIPE:
            return self.process_mock_license(workspace_identifier, license_details)

        workspace = self.workspace_repo.get_by_identifier(workspace_identifier)
        self.ensure_user_has_permission(user, workspace)
        product_lines = ProductLine.from_dict(self.product_repo, license_details["products"])
        license_details = LicenseDetails.from_product_lines(product_lines)
        self.ensure_valid_products(workspace, license_details)
        self.ensure_can_update_now(user)
        self.stripe_updater.remove_coupon_if_already_used(user)
        if self.is_trial(workspace_identifier):
            return self.stripe_updater.create_new_about_trial(
                user=user, product_lines=product_lines
            )
        return self.stripe_updater.update_subscription(
            user=user, product_lines=product_lines, proration_date=proration_date
        )

    def ensure_can_update_now(self, user: UserORM):
        last_invoice = self.stripe_updater.get_last_open_invoice(user)
        ok_states = {"paid"}
        if last_invoice and last_invoice.status not in ok_states:
            if last_invoice.hosted_invoice_url:
                raise LicenseRequiresAction(last_invoice.hosted_invoice_url)
            raise InvoicePending(last_invoice.id)

    def is_trial(self, workspace_identifier: Identifier) -> bool:
        workspace = self.workspace_repo._search_by_identifier(workspace_identifier)
        data = workspace.license
        products = data["details"]["products"]
        return any(
            product["identifier"] == envars.VITE_REACT_APP_PRODUCT_TRIAL for product in products
        )

    def ensure_can_update_now(self, user: UserORM):  # noqa E501
        last_invoice = self.stripe_updater.get_last_open_invoice(user)
        ok_states = {"paid"}
        if last_invoice and last_invoice.status not in ok_states:
            if last_invoice.hosted_invoice_url:
                raise LicenseRequiresAction(last_invoice.hosted_invoice_url)
            raise InvoicePending(last_invoice.id)

    def ensure_valid_products(self, workspace: WorkspaceORM, license_details: LicenseDetails):
        self.ensure_valid_quantities(license_details)
        self.ensure_no_product_duplication(license_details)

    def ensure_valid_quantities(self, license_details: LicenseDetails):
        if any(product_line.quantity <= 0 for product_line in license_details.product_lines):
            raise NegativeQuantity()

    def ensure_no_product_duplication(self, license_details: LicenseDetails):
        product_identifiers = tuple(
            product_line.product.identifier for product_line in license_details.product_lines
        )
        if len(product_identifiers) != len(set(product_identifiers)):
            raise DuplicatedProduct()

    def ensure_single_package(self, license_details: LicenseDetails):
        product_identifiers = (
            product_line.product.identifier for product_line in license_details.product_lines
        )
        products = self.product_repo.get_by_identifiers(product_identifiers)
        packages = tuple(
            product for product in products if product.characteristics.get("is_package")
        )
        if len(packages) != 1:
            raise NoSinglePackageProvided()

    def ensure_is_valid_downgrade(
        self, workspace: WorkspaceORM, new_license_details: LicenseDetails
    ):
        current_license_used_details = self.license_repo.get_current_used_characteristics(workspace)
        if (
            new_license_details.max_companies != "unlimited"
            and new_license_details.max_companies < current_license_used_details.max_companies
        ):
            raise CompaniesExceedNewLimit(
                current=new_license_details.max_companies,
                intended=new_license_details.max_companies,
            )
        if (
            new_license_details.max_emails_enroll != "unlimited"
            and new_license_details.max_emails_enroll
            < current_license_used_details.max_emails_enroll
        ):
            raise UsersExceedNewLimit(
                current_license_used_details.max_emails_enroll,
                new_license_details.max_emails_enroll,
            )

    def ensure_user_has_permission(self, user: UserORM, workspace: WorkspaceORM):
        if not self.license_repo.user_has_permission_to_modify(user, workspace):
            raise NotLicenseUpdateAllowed(user=user, workspace=workspace)
