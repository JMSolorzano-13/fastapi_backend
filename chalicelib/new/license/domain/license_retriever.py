import json
from dataclasses import dataclass
from datetime import datetime

from chalicelib.new.shared.domain.primitives import Identifier
from chalicelib.new.stripe.infra.stripe_coupon_setter import StripeCouponSetter
from chalicelib.new.stripe.infra.stripe_subscription_updater import StripeSubscriptionUpdater
from chalicelib.new.workspace.domain import WorkspaceRepository
from chalicelib.schema.models import User as UserORM
from chalicelib.schema.models import Workspace as WorkspaceORM

from .license_repository import LicenseRepository


class LicenseRetrieveException(Exception):
    pass


@dataclass
class AccessDenied(LicenseRetrieveException):
    user: UserORM
    workspace: WorkspaceORM

    def __str__(self) -> str:
        return (
            f"User not allowed to get license: "
            f"user={self.user.identifier}, workspace={self.workspace.identifier}"
        )


@dataclass
class LinesDetails:
    amount: int
    description: str
    quantity: int
    price: dict


@dataclass
class LatestOpenInvoice:
    status: str
    hosted_invoice_url: str
    created_at: str
    amount_due: int = None
    lines_data: list[LinesDetails] = None


@dataclass
class LatestPaidInvoice:
    lines_data: list[LinesDetails]


@dataclass
class CouponsInfo:
    id: str
    object: str
    amount_off: int
    created: str
    currency: str
    duration: str
    duration_in_months: int
    livemode: bool
    max_redemptions: int
    metadata: dict
    name: str
    percent_off: int
    redeem_by: str
    times_redeemed: int
    valid: bool


@dataclass
class LicenseInfo:
    sub_identifier: str
    cus_identifier: str
    add_enabled: bool = False
    any_invoice_paid: bool = False
    valid_until: int = None
    last_date_invoice: int = None
    last_charge_amount: int = None
    latest_invoice: LatestOpenInvoice = None
    latest_paid_invoice: LatestPaidInvoice = None
    details_coupon: CouponsInfo = None


@dataclass
class LicenseRetriever:
    license_repo: LicenseRepository
    workspace_repo: WorkspaceRepository
    stripe_updater: StripeSubscriptionUpdater
    stripe_coupon_setter: StripeCouponSetter = None

    def get_license_details(self, user: UserORM, workspace_identifier: Identifier) -> LicenseInfo:
        workspace = self.workspace_repo.get_by_identifier(workspace_identifier)
        self.ensure_user_has_permission(user, workspace)
        apportionment_details = self.stripe_updater.get_apportionment(user)
        latest_open_invoice = self.stripe_updater.get_last_open_invoice(user)
        latest_paid_invoice = self.stripe_updater.get_last_paid_invoice(user)
        lines_paid_str = json.dumps(latest_paid_invoice.lines.data) if latest_paid_invoice else None
        lines_open_str = json.dumps(latest_open_invoice.lines.data) if latest_open_invoice else None
        any_invoice_paid = self.stripe_updater.is_any_invoice_paid(user)
        stripe_coupon_setter = self.stripe_coupon_setter or StripeCouponSetter()
        coupons_details = stripe_coupon_setter.get_details_coupon(user.stripe_identifier)
        coupons_details = (
            json.dumps(coupons_details.discount.coupon) if coupons_details.discount else None
        )
        return LicenseInfo(
            sub_identifier=user.stripe_subscription_identifier,
            cus_identifier=user.stripe_identifier,
            add_enabled=user.workspace.add_permission or False,
            any_invoice_paid=any_invoice_paid,
            valid_until=apportionment_details.valid_until,
            last_date_invoice=apportionment_details.last_date_invoice,
            last_charge_amount=apportionment_details.last_charge_amount,
            latest_invoice=LatestOpenInvoice(
                status=latest_open_invoice.status,
                amount_due=latest_open_invoice.amount_due,
                hosted_invoice_url=latest_open_invoice.hosted_invoice_url,
                created_at=datetime.fromtimestamp(latest_open_invoice.created).isoformat(),
                lines_data=json.loads(lines_open_str) if lines_open_str else None,
            )
            if latest_open_invoice
            else None,
            latest_paid_invoice=LatestPaidInvoice(lines_data=json.loads(lines_paid_str))
            if latest_paid_invoice
            else None,
            details_coupon=json.loads(coupons_details) if coupons_details else None,
        )

    def ensure_user_has_permission(self, user: UserORM, workspace: WorkspaceORM):
        if not self.license_repo.user_has_permission_to_modify(user, workspace):
            raise AccessDenied(user=user, workspace=workspace)
