from dataclasses import dataclass
from typing import Any

import stripe

from chalicelib.new.config.infra import envars
from chalicelib.schema.models import User as UserORM

from .stripe_config import StripeConfig


@dataclass
class StripeAccountCreator(StripeConfig):
    def new_account(self, user: UserORM) -> dict[str, Any]:
        coupon = envars.STRIPE_COUPON if user.source_name else None
        customer = stripe.Customer.create(
            description=user.email,  # TODO
            email=user.email,
            metadata={
                "user_id": user.id,
                "odoo_id": user.odoo_identifier,
            },
            name=user.name,
            coupon=coupon,
        )
        user.stripe_identifier = customer["id"]
        return customer
