from dataclasses import dataclass
from typing import Any

import stripe

from chalicelib.new.config.infra import envars

from .stripe_config import StripeConfig


@dataclass
class StripeCouponSetter(StripeConfig):
    def get_customer(self, stripe_id: str) -> dict[str, Any]:
        return stripe.Customer.retrieve(stripe_id)

    def remove_coupon(self, stripe_id: str) -> dict[str, Any]:
        customer = self.get_customer(stripe_id)
        if customer.discount:
            stripe.Customer.delete_discount(stripe_id)

    def set_coupon(self, stripe_id: str) -> dict[str, Any]:
        invoices = stripe.Invoice.list(customer=stripe_id).data
        invoices = invoices or []
        already_paid = any(
            invoice.status == "paid" and invoice.amount_due > 0 for invoice in invoices
        )
        if already_paid:
            return
        stripe.Customer.modify(
            stripe_id,
            coupon=envars.STRIPE_COUPON,
        )

    def get_details_coupon(self, stripe_id: str) -> dict[str, Any]:
        details_coupons = stripe.Customer.retrieve(stripe_id)
        return details_coupons if details_coupons else None
