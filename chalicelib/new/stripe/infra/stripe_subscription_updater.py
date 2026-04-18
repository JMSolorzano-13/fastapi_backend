from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from logging import DEBUG

import stripe

from chalicelib.logger import log
from chalicelib.modules import Modules
from chalicelib.new.license.domain.product_line import ProductLine
from chalicelib.new.stripe.infra.stripe_subscription_creator import (
    StripeSubscriptionCreator,
)
from chalicelib.schema.models import User as UserORM

from ...config.infra import envars
from .stripe_config import StripeConfig

InvoiceURL = str


@dataclass
class Apportionment_details:
    last_charge_amount: int = None
    last_date_invoice: int = None
    valid_until: int = None


@dataclass
class StripeSubscriptionUpdater(StripeConfig):
    def get_last_open_invoice(self, user: UserORM) -> stripe.Invoice:
        if not user.stripe_subscription_identifier:
            return ""
        current_subscription = stripe.Subscription.retrieve(user.stripe_subscription_identifier)
        last_invoice_to_pay = stripe.Invoice.list(customer=user.stripe_identifier, status="open")
        return (
            stripe.Invoice.retrieve(last_invoice_to_pay.data[0].id)
            if current_subscription and last_invoice_to_pay
            else ""
        )

    def get_last_paid_invoice(self, user: UserORM) -> stripe.Invoice:
        if not user.stripe_subscription_identifier:
            return ""
        current_subscription = stripe.Subscription.retrieve(user.stripe_subscription_identifier)
        last_invoice_paid = stripe.Invoice.list(customer=user.stripe_identifier, status="paid")
        return (
            stripe.Invoice.retrieve(last_invoice_paid.data[0].id)
            if current_subscription and last_invoice_paid
            else ""
        )

    def get_apportionment(self, user: UserORM) -> Apportionment_details:
        subscription = stripe.Subscription.retrieve(user.stripe_subscription_identifier)
        if not subscription:
            return Apportionment_details()
        invoice_list = stripe.Invoice.list(customer=user.stripe_identifier).data
        charges_list = stripe.Charge.list(customer=user.stripe_identifier, status="all")
        last_date_invoice = max(invoice_list or [0], key=lambda x: x.created if x else None)
        last_charge_amount = 0
        if charges_list.data:
            charges_paid = [charge for charge in charges_list.data if charge.paid]
            if charges_paid:
                last_charge = max(
                    charges_paid,
                    key=lambda r: r.created,
                )
                last_charge_amount = last_charge.amount

        return Apportionment_details(
            last_date_invoice=last_date_invoice.created if last_date_invoice else None,
            valid_until=subscription.current_period_end if subscription else None,
            last_charge_amount=last_charge_amount or None,
        )

    def is_trial_end(self, user: UserORM) -> bool:
        subscription = stripe.Subscription.retrieve(user.stripe_subscription_identifier)
        status = subscription.status
        return status != "active"

    def create_new_about_trial(
        self, user: UserORM, product_lines: Iterable[ProductLine]
    ) -> InvoiceURL:
        creator = StripeSubscriptionCreator()
        if not self.is_trial_end(user):
            stripe.Subscription.delete(user.stripe_subscription_identifier)
        items = (
            (product.product.stripe_price_identifier, product.quantity) for product in product_lines
        )
        return creator.new_subscription(user, items=items), 200

    def can_update_subscription(self, subscription) -> bool:
        return subscription.status != "canceled"

    def update_subscription(
        self, user: UserORM, product_lines: Iterable[ProductLine], proration_date: int = None
    ) -> InvoiceURL:
        actions = []
        subscription = stripe.Subscription.retrieve(user.stripe_subscription_identifier)
        customer_id = subscription.customer
        status = subscription.status
        log(
            Modules.STRIPE,
            DEBUG,
            "UPDATE_SUBSCRIPTION",
            {"subscription_id": user.stripe_subscription_identifier},
        )

        if status != "active":
            creator = StripeSubscriptionCreator()
            items = (
                (product.product.stripe_price_identifier, product.quantity)
                for product in product_lines
            )
            return creator.new_subscription(user, items=items), 200

        for product_line in product_lines:
            new_product = product_line.product
            quantity = product_line.quantity
            subscription_items = stripe.SubscriptionItem.list(
                subscription=user.stripe_subscription_identifier
            )
            product = stripe.Product.retrieve(new_product.stripe_identifier)
            default_price = stripe.Price.retrieve(product.default_price)
            metadata = product.metadata

            if "is_package" in metadata:
                for item in subscription_items.data:
                    product = stripe.Product.retrieve(item.price.product)
                    metadata_product = product.metadata
                    if "is_package" in metadata_product:
                        if (
                            item.price.unit_amount > default_price.unit_amount
                            or quantity == 0
                            or quantity > 1
                        ):
                            return (
                                "No se puede agregar un paquete con un precio menor al que ya tiene o con una cantidad mayor a 1",  # noqa E501
                                400,
                            )
                        actions.append(
                            {
                                "action": "delete",
                                "id": item.id,
                            }
                        )
                        break

            update = False
            for item in subscription_items.data:
                if item.price.product == new_product.stripe_identifier:
                    if quantity <= item.quantity:
                        return (
                            "No se puede agregar una cantidad menor o igual a la que ya tiene",
                            400,
                        )
                    actions.insert(0, {"action": "update", "id": item.id, "quantity": quantity})
                    update = True
                    break

            if not update:
                actions.insert(
                    0, {"action": "create", "price": default_price.id, "quantity": quantity}
                )

        for action in actions:
            if action["action"] == "delete":
                stripe.SubscriptionItem.delete(
                    action["id"],
                    proration_date=proration_date,
                )
            elif action["action"] == "update":
                stripe.SubscriptionItem.modify(
                    action["id"],
                    quantity=action["quantity"],
                    proration_date=proration_date,
                )
            elif action["action"] == "create":
                stripe.SubscriptionItem.create(
                    subscription=user.stripe_subscription_identifier,
                    price=action["price"],
                    quantity=action["quantity"],
                    proration_date=proration_date,
                )

        invoice = stripe.Invoice.create(
            customer=customer_id,
            subscription=user.stripe_subscription_identifier,
            auto_advance=False,
            payment_settings={"payment_method_types": ["card", "customer_balance"]},
        )

        stripe.Invoice.finalize_invoice(invoice.id)
        log(
            Modules.STRIPE,
            DEBUG,
            "INVOICE_FINALIZED",
            {"invoice_id": invoice.id},
        )
        invoice_link = (
            stripe.Invoice.list(subscription=user.stripe_subscription_identifier, status="open")
            .data[0]
            .hosted_invoice_url
        )

        return invoice_link or None, 200

    def renew_trial(self, user: UserORM):
        if not user.stripe_subscription_identifier:
            return
        current_subscription = stripe.Subscription.retrieve(user.stripe_subscription_identifier)
        today = datetime.date.today()
        trial_end = today + envars.STRIPE_DEFAULT_CANCEL_AT_DELTA
        if current_subscription.trial_end > today:
            stripe.Subscription.modify(
                user.stripe_subscription_identifier,
                trial_end=trial_end,
            )
        else:
            stripe.Subscription.delete(user.stripe_subscription_identifier)
            stripe_account_creator = StripeSubscriptionCreator()
            stripe_account_creator.new_subscription(
                user, cancel_delta=envars.STRIPE_DEFAULT_CANCEL_AT_DELTA
            )

    def _update_current_subscription(  # pylint: disable=too-many-locals
        self,
        user: UserORM,
        product_lines: Iterable[ProductLine],
        current_subscription,
        product_lines_dict,
        product_prices,
    ) -> InvoiceURL:
        current_product_id_and_sub_id = {
            item["price"]["product"]: item["id"] for item in current_subscription["items"]["data"]
        }
        intended_product_ids = {line.product.stripe_identifier for line in product_lines}
        deleted_product_ids = set(current_product_id_and_sub_id.keys()) - intended_product_ids
        new_product_ids = intended_product_ids - set(current_product_id_and_sub_id.keys())
        updated_product_ids = intended_product_ids & set(current_product_id_and_sub_id.keys())
        deleted_lines = [
            {"id": current_product_id_and_sub_id[product_id], "deleted": True}
            for product_id in deleted_product_ids
        ]
        updated_lines = [
            {
                "id": current_product_id_and_sub_id[product_id],
                "quantity": product_lines_dict[product_id],
            }
            for product_id in updated_product_ids
        ]
        new_lines = [
            {
                "price": product_prices[product_id],
                "quantity": product_lines_dict[product_id],
            }
            for product_id in new_product_ids
        ]

        items = [
            *updated_lines,
            *deleted_lines,
            *new_lines,
        ]
        res = stripe.Subscription.modify(
            user.stripe_subscription_identifier,
            proration_behavior="always_invoice",
            trial_end="now",
            items=items,
            billing_cycle_anchor="now",
            cancel_at_period_end=False,
        )
        stripe.Invoice.finalize_invoice(res.latest_invoice)
        invoice = stripe.Invoice.retrieve(res.latest_invoice)
        return invoice.hosted_invoice_url

    def remove_coupon_if_already_used(self, user: UserORM):
        if user.source_name is None:
            return
        invoices = stripe.Invoice.list(customer=user.stripe_identifier).data
        if invoices is None:
            return
        already_paid = any(
            invoice.status == "paid" and invoice.amount_due > 0 for invoice in invoices
        )
        customer = stripe.Customer.retrieve(user.stripe_identifier)
        if customer.discount is not None and (already_paid or envars.stripe.SEASONAL_DISCOUNT):
            stripe.Customer.delete_discount(user.stripe_identifier)

    def is_any_invoice_paid(self, user: UserORM) -> bool:
        invoices = stripe.Invoice.list(customer=user.stripe_identifier).data
        if invoices is None:
            return False
        return any(invoice.status == "paid" and invoice.amount_due > 0 for invoice in invoices)
