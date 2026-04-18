"""Product/Stripe helper functions extracted from chalicelib/blueprints/product.py.

Avoids importing through the blueprints package (which triggers Chalice).
Used by routers/product.py and routers/license_bp.py.
"""

import stripe as stripeAPI

from chalicelib.logger import EXCEPTION, Modules, log
from chalicelib.new.config.infra import envars


def get_list_of_products(isRenew: int):
    if envars.mock.STRIPE:
        return _get_mock_products(isRenew)

    productos_filtrados = []

    stripeAPI.api_key = envars.STRIPE_SECRET_KEY
    productos = stripeAPI.Product.list(active=True)

    for producto in productos.data:
        precios = stripeAPI.Price.list(product=producto.id)
        precios_no_publicos = [
            precio for precio in precios.data if precio.metadata.get("isRenew") == str(isRenew)
        ]

        for precio in precios_no_publicos:
            producto_info = {
                "identifier": producto.id,
                "execute_at": None,
                "stripe_identifier": producto.id,
                "characteristics": producto.metadata.to_dict(),
                "price": precio.unit_amount,
                "stripe_price_identifier": precio.id,
                "stripe_name": producto.name,
            }
            productos_filtrados.append(producto_info)

    return {"products": productos_filtrados}


def _get_mock_products(isRenew: int):
    mock_products = [
        {
            "identifier": "prod_MjDE9ihnCFzJn7",
            "execute_at": None,
            "stripe_identifier": "prod_MjDE9ihnCFzJn7",
            "characteristics": {
                "max_companies": "1",
                "max_emails_enroll": "1",
                "add_enabled": "false",
                "exceed_metadata_limit": "false",
            },
            "price": 99900,
            "stripe_price_identifier": "price_1MockBasicPrice",
            "stripe_name": "Plan Básico - Local Dev",
        },
        {
            "identifier": "prod_MockProfessional",
            "execute_at": None,
            "stripe_identifier": "prod_MockProfessional",
            "characteristics": {
                "max_companies": "5",
                "max_emails_enroll": "5",
                "add_enabled": "true",
                "exceed_metadata_limit": "false",
            },
            "price": 199900,
            "stripe_price_identifier": "price_2MockProfessionalPrice",
            "stripe_name": "Plan Profesional - Local Dev",
        },
        {
            "identifier": "prod_MockEnterprise",
            "execute_at": None,
            "stripe_identifier": "prod_MockEnterprise",
            "characteristics": {
                "max_companies": "999",
                "max_emails_enroll": "999",
                "add_enabled": "true",
                "exceed_metadata_limit": "true",
            },
            "price": 499900,
            "stripe_price_identifier": "price_3MockEnterprisePrice",
            "stripe_name": "Plan Empresarial - Local Dev",
        },
    ]
    return {"products": mock_products}


def get_latest_subscription(customer_id):
    try:
        all_subscriptions = []

        subscriptions_active = stripeAPI.Subscription.list(
            customer=customer_id,
            status="active",
        )
        all_subscriptions.extend(subscriptions_active.data)

        subscriptions_past_due = stripeAPI.Subscription.list(
            customer=customer_id,
            status="past_due",
        )
        all_subscriptions.extend(subscriptions_past_due.data)

        subscriptions_canceled = stripeAPI.Subscription.list(
            customer=customer_id,
            status="canceled",
        )
        all_subscriptions.extend(subscriptions_canceled.data)

        if all_subscriptions:
            return max(all_subscriptions, key=lambda sub: sub.created)
        else:
            return None

    except stripeAPI.error.StripeError:
        log(
            Modules.STRIPE,
            EXCEPTION,
            "get_subscription_error",
        )
        return None


def get_all_products_from_subscription(subscription_id):
    subscription = stripeAPI.Subscription.retrieve(subscription_id)

    product_ids = []

    items = subscription.items().mapping["items"]["data"]

    for item in items:
        product_ids.append(item["plan"]["product"])

    return product_ids
