from dataclasses import dataclass
from logging import WARNING
from typing import Any

import stripe
from stripe.api_resources.product import Product as StripeProduct

from chalicelib.logger import log
from chalicelib.modules import Modules
from chalicelib.new.product.domain import Product
from chalicelib.new.stripe.infra import StripeConfig


@dataclass
class ProductStripeConstructor(StripeConfig):
    secret: str

    def _get_stripe_product(self, payload: str, signature: str) -> StripeProduct:
        event = stripe.Webhook.construct_event(payload, signature, self.secret)

        # Handle the event
        if event["type"] in ("product.created", "product.updated"):
            return event["data"]["object"]
        log(
            Modules.STRIPE,
            WARNING,
            "WEBHOOK_UNHANDLED_EVENT_TYPE",
            {"event_type": event["type"]},
        )
        raise ValueError(f"Unhandled event type {event['type']}")

    def sanitize_characteristics(self, characteristics: dict[str, Any]) -> None:
        max_emails_enroll = characteristics.get("max_emails_enroll", 0)
        if isinstance(max_emails_enroll, str) and max_emails_enroll.isdigit():
            characteristics["max_emails_enroll"] = int(max_emails_enroll)
        max_companies = characteristics.get("max_companies", 0)
        if isinstance(max_companies, str) and max_companies.isdigit():
            characteristics["max_companies"] = int(max_companies)

    def construct(self, payload: str, signature: str) -> Product:
        stripe_product = self._get_stripe_product(payload, signature)

        price = stripe.Price.retrieve(stripe_product.default_price)
        self.sanitize_characteristics(stripe_product.metadata)

        return Product(
            stripe_identifier=stripe_product.id,
            characteristics=stripe_product.metadata,
            price=price.unit_amount,
            stripe_price_identifier=stripe_product.default_price,
            stripe_name=stripe_product.name,
        )
