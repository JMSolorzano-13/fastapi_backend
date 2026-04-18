import stripe as stripeAPI
from sqlalchemy.orm import Session

from chalicelib.blueprints import common
from chalicelib.blueprints.superblueprint import SuperBlueprint
from chalicelib.logger import EXCEPTION, Modules, log
from chalicelib.new.config.infra import envars
from chalicelib.new.product.domain import ProductSetter
from chalicelib.new.product.infra import ProductRepositorySA
from chalicelib.new.product.infra.product_stripe_constructor import (
    ProductStripeConstructor,
)

bp = SuperBlueprint(__name__)


@bp.route("/set", methods=["POST"], cors=common.cors_config, read_only=False)
def set_product(session: Session):
    payload = bp.current_request.raw_body
    signature = bp.current_request.headers["stripe-signature"]

    # TODO security

    product_repo = ProductRepositorySA(session)
    product_setter = ProductSetter(product_repo)
    product_constructor = ProductStripeConstructor(envars.STRIPE_SET_PRODUCT_SECRET_KEY)
    product = product_constructor.construct(payload, signature)
    product_setter.set_product(product)


@bp.route(
    "/get_all",
    methods=["GET"],
    cors=common.cors_config,
)
def search(session: Session):
    request = bp.current_request
    token = request.headers.get("access_token")
    if not token:
        return 401

    return _get_products(session=session, token=token)


def _get_products(session, token):
    # 0 is normal price, 1 is renew price
    return get_list_of_products(0)


def get_list_of_products(isRenew: int):
    """
    Get list of products from Stripe.
    In mock mode (MOCK_STRIPE=1), returns mock products instead of calling Stripe API.
    """
    # In mock Stripe mode, return mock products
    if envars.mock.STRIPE:
        return _get_mock_products(isRenew)
    
    # Production: Call actual Stripe API
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

    resultado = {"products": productos_filtrados}
    return resultado


def _get_mock_products(isRenew: int):
    """
    Return mock products for local development.
    Simulates Stripe product structure without requiring actual Stripe API calls.
    """
    mock_products = [
        {
            "identifier": "prod_MjDE9ihnCFzJn7",
            "execute_at": None,
            "stripe_identifier": "prod_MjDE9ihnCFzJn7",
            "characteristics": {
                "max_companies": "1",
                "max_emails_enroll": "1",
                "add_enabled": "false",
                "exceed_metadata_limit": "false"
            },
            "price": 99900,  # $999.00 in cents
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
                "exceed_metadata_limit": "false"
            },
            "price": 199900,  # $1,999.00 in cents
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
                "exceed_metadata_limit": "true"
            },
            "price": 499900,  # $4,999.00 in cents
            "stripe_price_identifier": "price_3MockEnterprisePrice",
            "stripe_name": "Plan Empresarial - Local Dev",
        },
    ]
    
    # Filter by isRenew if needed (for local dev, we'll return all products)
    resultado = {"products": mock_products}
    return resultado


def get_latest_subscription(customer_id):
    """
    Obtiene la suscripción más reciente de un cliente en Stripe.

    :param customer_id: El ID del cliente en Stripe.
    :return: La suscripción más reciente o None si no hay suscripciones.
    """
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
            return None  # No hay suscripciones

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
