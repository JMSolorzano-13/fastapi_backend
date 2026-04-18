from dataclasses import dataclass

import stripe

from chalicelib.new.config.infra import envars


@dataclass
class StripeConfig:
    def __post_init__(self):
        stripe.api_key = envars.STRIPE_SECRET_KEY
