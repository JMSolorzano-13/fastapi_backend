from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import stripe

from chalicelib.logger import DEBUG, log
from chalicelib.modules import Modules
from chalicelib.new.config.infra import envars
from chalicelib.new.product.infra.product_repository_sa import ProductRepositorySA
from chalicelib.schema.models import User as UserORM

from .stripe_config import StripeConfig

License = dict[str, Any]


@dataclass
class StripeSubscriptionCreator(StripeConfig):
    def new_sub_from_current_license(
        self,
        user: UserORM,
        license_dict: License,
        product_repo: ProductRepositorySA,
    ) -> stripe.Subscription:
        date_start = datetime.fromisoformat(license_dict["date_start"])
        date_end = datetime.fromisoformat(license_dict["date_end"])
        products_required = license_dict["details"]["products"]
        products = product_repo.get_by_identifiers(
            [product["identifier"] for product in products_required]
        )
        products = {product.identifier: product.stripe_price_identifier for product in products}
        items = (
            (products[product["identifier"]], int(product["quantity"]))
            for product in products_required
        )
        return self.new_subscription(
            user,
            items=items,
            proration_behavior=envars.STRIPE_DEFAULT_PRORATION_BEHAVIOR_CURRENT_CUSTOMERS,
            date_start=date_start,
            date_end=date_end,
        )

    def change_started_subscription(
        self, user: UserORM, license: License, product_repo: ProductRepositorySA
    ) -> None:
        customer = user.stripe_identifier
        subscriptions = stripe.Subscription.list(customer=customer, status="all")
        charge_list = stripe.Charge.list(customer=customer, status="all")
        invoice = stripe.Invoice.list(customer=customer, status="open").data

        total_charges = [
            charge
            for charge in charge_list.auto_paging_iter()
            if charge.paid and charge.status == "succeeded"
        ]

        has_trial = any(
            subs.plan and subs.plan.product == envars.VITE_REACT_APP_PRODUCT_TRIAL
            for subs in subscriptions.auto_paging_iter()
        )

        has_no_trial = any(
            subs.plan and subs.plan.product != envars.VITE_REACT_APP_PRODUCT_TRIAL
            for subs in subscriptions.auto_paging_iter()
        )

        change_blockers = (
            (not charge_list, "NO_CHARGES_FOUND"),
            (not subscriptions, "NO_SUBSCRIPTIONS_FOUND"),
            (
                license["stripe_status"] in ["cancelled", "canceled"],
                "LICENSE_CANCELLED",
            ),
            (license["stripe_status"] == "past_due", "LICENSE_PAST_DUE"),
            (len(subscriptions) > 1 and len(total_charges) == 1, "MULTIPLE_SUBSCRIPTIONS_FOUND"),
            (len(total_charges) != 1, "NO_SINGLE_CHARGE_FOUND"),
            (not invoice, "INVOICE_NOT_FOUND"),
            (not has_trial, "NO_TRIAL_SUBSCRIPTION_FOUND"),
            (not has_no_trial, "NO_SUBSCRIPTIONS_FOUND"),
        )
        for blocker, log_code in change_blockers:
            if blocker:
                log(
                    Modules.STRIPE,
                    DEBUG,
                    log_code,
                    {
                        "customer": customer,
                        "license": license,
                    },
                )
                return

        stripe.Subscription.delete(user.stripe_subscription_identifier)
        products = product_repo.get_by_identifiers(
            [product["identifier"] for product in license["details"]["products"]]
        )
        price_by_product = {
            product.identifier: product.stripe_price_identifier for product in products
        }
        items = (
            (price_by_product[product["identifier"]], int(product["quantity"]))
            for product in license["details"]["products"]
        )
        today = datetime.utcnow()
        date_start = today.replace(hour=0, minute=0, second=0, microsecond=0)
        next_year = today.replace(year=today.year + 1)
        self.new_subscription(
            user,
            items=items,
            proration_behavior=envars.STRIPE_DEFAULT_PRORATION_BEHAVIOR_CURRENT_CUSTOMERS,
            date_start=date_start,
            date_end=next_year,
        )

    def new_subscription(
        self,
        user: UserORM,
        items: list[tuple[str, int]] = envars.STRIPE_DEFAULT_ITEMS,
        days_until_due: int = envars.STRIPE_DAYS_UNTIL_DUE,
        default_tax_rates: list[str] = envars.STRIPE_DEFAULT_TAX_RATES,
        proration_behavior: str = envars.STRIPE_DEFAULT_PRORATION_BEHAVIOR,
        date_start: datetime = None,
        date_end: datetime = None,
        cancel_delta: timedelta = None,
    ) -> stripe.Subscription:
        log(
            Modules.STRIPE,
            DEBUG,
            "NEW_SUBSCRIPTION",
            {"user_id": user.id},
        )
        optional_params = {}
        if date_start:
            optional_params["backdate_start_date"] = int(date_start.timestamp())
        if date_end:
            optional_params["billing_cycle_anchor"] = int(date_end.timestamp())
        if cancel_delta:
            today = datetime.now()
            optional_params["cancel_at"] = int((today + cancel_delta).timestamp())

        subscription = stripe.Subscription.create(
            customer=user.stripe_identifier,
            items=[
                {
                    "price": item[0],
                    "quantity": item[1],
                }
                for item in items
            ],
            days_until_due=days_until_due,
            default_tax_rates=default_tax_rates,
            collection_method="send_invoice",
            metadata={
                "user_id": user.id,
                "odoo_id": user.odoo_identifier,
            },
            payment_settings={"payment_method_types": ["customer_balance", "card"]},
            proration_behavior=proration_behavior,
            **optional_params,
        )

        user.stripe_subscription_identifier = subscription["id"]

        current_subscription = stripe.Subscription.retrieve(user.stripe_subscription_identifier)
        if current_subscription.latest_invoice:
            invoice = stripe.Invoice.finalize_invoice(current_subscription.latest_invoice)
            invoice_url = invoice.hosted_invoice_url
            return invoice_url if invoice else ""

        if not subscription.latest_invoice:
            return ""
        invoice = stripe.Invoice.retrieve(subscription.latest_invoice)
        log(
            Modules.STRIPE,
            DEBUG,
            "INVOICE_RETRIEVED",
            {"invoice_id": current_subscription.latest_invoice},
        )
        return invoice.hosted_invoice_url if invoice else ""
