"""License routes — subscription management, Stripe integration, free trial.

Ported from: backend/chalicelib/blueprints/license_bp.py
6 routes total.
"""

from dataclasses import asdict

import stripe as stripeAPI
from fastapi import APIRouter, Body, Depends, Request
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from chalicelib.controllers import scale_to_super_user
from chalicelib.controllers.user import UserController
from chalicelib.controllers.workspace import WorkspaceController
from chalicelib.logger import DEBUG, EXCEPTION, log
from chalicelib.modules import Modules
from chalicelib.new.config.infra.envars import envars
from chalicelib.new.license.domain.license_retriever import AccessDenied, LicenseRetriever
from chalicelib.new.license.domain.license_setter import LicenseSetter, LicenseUpdateException
from chalicelib.new.license.infra import LicenseRepositorySA
from chalicelib.new.license.infra.siigo_marketing import get_siigo_free_trial
from chalicelib.new.product.infra import ProductRepositorySA
from chalicelib.new.source import SourceSetter
from chalicelib.new.stripe.infra.stripe_subscription_updater import (
    StripeSubscriptionUpdater,
)
from chalicelib.new.workspace.infra import WorkspaceRepositorySA
from chalicelib.schema.models.user import User
from chalicelib.schema.models.workspace import Workspace
from dependencies import (
    get_current_user,
    get_current_user_rw,
    get_db_session,
    get_db_session_rw,
)
from exceptions import BadRequestError, NotFoundError, UnauthorizedError
from helpers.product_helpers import get_latest_subscription, get_list_of_products

router = APIRouter(tags=["License"])


@router.put("", include_in_schema=False)
@router.put("/")
def put(
    body: dict = Body(...),
    session: Session = Depends(get_db_session_rw),
    user: User = Depends(get_current_user_rw),
):
    licenses = body.get("licenses", [])

    context = {"user": user}
    scale_to_super_user(context)

    WorkspaceController.set_licenses(licenses, session=session, context=context)


@router.post("/paid/alert")
async def get_alert_invoice_paid(request: Request):
    payload = (await request.body()).decode("utf-8")
    sig_header = request.headers.get("Stripe-Signature")
    stripeAPI.api_key = envars.STRIPE_SECRET_KEY

    try:
        event = stripeAPI.Webhook.construct_event(
            payload, sig_header, envars.STRIPE_WEBHOOK_PAID_ALERT
        )
    except stripeAPI.error.SignatureVerificationError:
        return JSONResponse(status_code=400, content={"error": "Invalid signature"})

    if event["type"] != "invoice.paid":
        return {"success": True}

    invoice = event["data"]["object"]

    if invoice.get("subscription") is None:
        return {"success": True}

    try:
        subscription_id = invoice["subscription"]
        subscription = stripeAPI.Subscription.retrieve(subscription_id)

        products_renew_price = get_list_of_products(1)

        renewal_price_map = {
            product["identifier"]: product["stripe_price_identifier"]
            for product in products_renew_price["products"]
        }

        updated_items = []
        for item in subscription["items"]["data"]:
            product_id = item["price"]["product"]
            renewal_price_id = renewal_price_map.get(product_id)
            quantity = item["quantity"]

            if renewal_price_id:
                updated_items.append(
                    {"id": item.id, "price": renewal_price_id, "quantity": quantity}
                )
            else:
                updated_items.append(
                    {"id": item.id, "price": renewal_price_id, "quantity": quantity}
                )

        if any(
            renewal_price_map.get(item["price"]["product"]) is not None
            for item in subscription["items"]["data"]
        ):
            stripeAPI.Subscription.modify(
                subscription_id,
                items=updated_items,
                proration_behavior="none",
            )
            log(
                Modules.STRIPE,
                DEBUG,
                "UPDATE_SUBSCRIPTION",
                {"subscription_id": subscription_id, "updated_items": updated_items},
            )
        else:
            log(
                Modules.STRIPE,
                DEBUG,
                "NO_RENEWAL_PRICES",
                {
                    "subscription_id": subscription_id,
                    "error": "No se encontraron precios para renovar",
                },
            )

    except Exception as e:
        log(
            Modules.STRIPE,
            EXCEPTION,
            "FAIL_UPDATE_SUBSCRIPTION",
            {"invoice": invoice, "error": str(e)},
        )

    return {"success": True}


@router.put("/source")
def set_source(
    body: dict = Body(...),
    session: Session = Depends(get_db_session_rw),
    user: User = Depends(get_current_user_rw),
):
    source = body.get("source")
    user_id = body.get("user_id")
    UserController.ensure_external_super_user(user, "set the source", session=session)
    try:
        setter = SourceSetter(session)
    except Exception as e:
        raise BadRequestError(str(e)) from e
    setter.set(user_id, source)


@router.post("", include_in_schema=False)
@router.post("/")
def get_license_details(
    body: dict = Body(...),
    session: Session = Depends(get_db_session),
    user: User = Depends(get_current_user),
):
    workspace_identifier = body.pop("workspace_identifier")
    license_repo = LicenseRepositorySA(session)
    workspace_repo = WorkspaceRepositorySA(session)
    stripe_updater = StripeSubscriptionUpdater()
    retriever = LicenseRetriever(license_repo, workspace_repo, stripe_updater)
    try:
        info = retriever.get_license_details(user, workspace_identifier)
        return asdict(info)
    except LicenseUpdateException as e:
        raise BadRequestError(str(e)) from e
    except AccessDenied as e:
        raise UnauthorizedError(str(e)) from e


@router.post("/set")
def set_license_by_user_v2(
    body: dict = Body(...),
    session: Session = Depends(get_db_session_rw),
    user: User = Depends(get_current_user_rw),
):
    return _set_license_by_user_v2(session=session, json_body=body, user=user)


def _set_license_by_user_v2(session, json_body: dict, user: User):
    product_list = get_list_of_products(0)

    stripeAPI.api_key = envars.STRIPE_SECRET_KEY

    workspace_identifier = json_body.pop("workspace_identifier")
    proration_date = json_body.pop("proration_date")
    matched_products = [
        {
            **product,
            "quantity": next(
                p["quantity"]
                for p in json_body["products"]
                if p["identifier"] == product["identifier"]
            ),
        }
        for product in product_list["products"]
        if product["identifier"] in {p["identifier"] for p in json_body["products"]}
    ]

    license_repo = LicenseRepositorySA(session)
    product_repo = ProductRepositorySA(session)
    workspace_repo = WorkspaceRepositorySA(session)
    stripe_updater = StripeSubscriptionUpdater()

    license_setter = LicenseSetter(license_repo, workspace_repo, product_repo, stripe_updater)

    workspace = workspace_repo.get_by_identifier(workspace_identifier)

    try:
        license_setter.ensure_user_has_permission(user, workspace)
        license_setter.ensure_can_update_now(user)
        stripe_updater.remove_coupon_if_already_used(user)
        invoice_url = _update_license(user, matched_products, proration_date)
        return JSONResponse(
            status_code=200,
            content={"invoice_url": invoice_url},
        )
    except LicenseUpdateException as e:
        raise BadRequestError(str(e)) from e


def _update_license(user, product_lines, proration_date):
    def new_subscription(
        user,
        items,
        default_tax_rates: list[str] = envars.STRIPE_DEFAULT_TAX_RATES,
        days_until_due: int = envars.STRIPE_DAYS_UNTIL_DUE,
        proration_behavior: str = envars.STRIPE_DEFAULT_PRORATION_BEHAVIOR,
    ):
        res_subscription = stripeAPI.Subscription.create(
            customer=user.stripe_identifier,
            items=[
                {
                    "price": item["stripe_price_identifier"],
                    "quantity": item["quantity"],
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
        )
        user.stripe_subscription_identifier = res_subscription["id"]
        current_subscription = get_latest_subscription(user.stripe_identifier)

        if current_subscription.latest_invoice:
            invoice = stripeAPI.Invoice.finalize_invoice(current_subscription.latest_invoice)
            invoice_url = invoice.hosted_invoice_url
            return invoice_url if invoice else ""
        if not current_subscription.latest_invoice:
            return ""
        invoice = stripeAPI.Invoice.retrieve(current_subscription.latest_invoice)
        return invoice.hosted_invoice_url if invoice else ""

    actions = []
    subscription = get_latest_subscription(user.stripe_identifier)
    status = subscription.status

    for products in subscription.items().mapping["items"]["data"]:
        if (
            products["price"]["product"] == envars.VITE_REACT_APP_PRODUCT_TRIAL
            and status == "active"
        ):
            stripeAPI.Subscription.cancel(subscription)
            return new_subscription(user, product_lines)

    if status != "active":
        return new_subscription(user, product_lines)
    _set_original_price(subscription.stripe_id)
    for product_line in product_lines:
        quantity = product_line["quantity"]
        subscription_items = stripeAPI.SubscriptionItem.list(subscription=subscription.stripe_id)
        product = stripeAPI.Product.retrieve(product_line["stripe_identifier"])
        price = stripeAPI.Price.retrieve(product_line["stripe_price_identifier"])
        metadata = product.metadata

        if "is_package" in metadata:
            for item in subscription_items.data:
                product = stripeAPI.Product.retrieve(item.price.product)
                metadata_product = product.metadata
                if "is_package" in metadata_product:
                    if item.price.unit_amount > price.unit_amount or quantity == 0 or quantity > 1:
                        return (
                            "No se puede agregar un paquete con un precio menor"
                            " al que ya tiene o con una cantidad mayor a 1",
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
            if item.price.product == product_line["stripe_identifier"]:
                if "is_package" not in metadata:
                    actions.insert(0, {"action": "update", "id": item.id, "quantity": quantity})
                    update = True
                    break
                if quantity <= item.quantity:
                    return (
                        "No se puede agregar una cantidad menor o igual a la que ya tiene",
                        400,
                    )
                actions.insert(0, {"action": "update", "id": item.id, "quantity": quantity})
                update = True
                break

        if not update:
            actions.insert(0, {"action": "create", "price": price.id, "quantity": quantity})
    for action in actions:
        if action["action"] == "delete":
            stripeAPI.SubscriptionItem.delete(
                action["id"],
                proration_date=proration_date,
            )
        elif action["action"] == "update":
            stripeAPI.SubscriptionItem.modify(
                action["id"],
                quantity=action["quantity"],
                proration_date=proration_date,
            )
        elif action["action"] == "create":
            stripeAPI.SubscriptionItem.create(
                subscription=subscription.stripe_id,
                price=action["price"],
                quantity=action["quantity"],
                proration_date=proration_date,
            )

    invoice = stripeAPI.Invoice.create(
        customer=user.stripe_identifier,
        subscription=subscription.stripe_id,
        auto_advance=True,
        payment_settings={"payment_method_types": ["card", "customer_balance"]},
    )

    stripeAPI.Invoice.finalize_invoice(invoice.id)
    return (
        stripeAPI.Invoice.list(subscription=subscription.stripe_id, status="open")
        .data[0]
        .hosted_invoice_url
    )


def _set_original_price(subscription_id):
    try:
        subscription = stripeAPI.Subscription.retrieve(subscription_id)

        products_renew_price = get_list_of_products(0)

        renewal_price_map = {
            product["identifier"]: product["stripe_price_identifier"]
            for product in products_renew_price["products"]
        }

        updated_items = []
        for item in subscription["items"]["data"]:
            product_id = item["price"]["product"]
            renewal_price_id = renewal_price_map.get(product_id)
            quantity = item["quantity"]

            if renewal_price_id:
                updated_items.append(
                    {"id": item.id, "price": renewal_price_id, "quantity": quantity}
                )
            else:
                updated_items.append(
                    {"id": item.id, "price": renewal_price_id, "quantity": quantity}
                )

        if any(
            renewal_price_map.get(item["price"]["product"]) is not None
            for item in subscription["items"]["data"]
        ):
            stripeAPI.Subscription.modify(
                subscription_id,
                items=updated_items,
                proration_behavior="none",
            )
        else:
            return

    except Exception as e:
        log(
            Modules.STRIPE,
            EXCEPTION,
            "FAIL_UPDATE_SUBSCRIPTION_SET_ORIGINAL",
            {"invoice": subscription_id, "error": str(e)},
        )


@router.get("/{workspace_identifier}")
def get_free_trial_by_workspace(
    workspace_identifier: str,
    session: Session = Depends(get_db_session),
):
    owner_email = (
        session.query(User.email)
        .join(Workspace, Workspace.owner_id == User.id)
        .filter(Workspace.identifier == workspace_identifier)
        .scalar()
    )

    if not owner_email:
        raise NotFoundError(
            f"No se encontró workspace '{workspace_identifier}' o no tiene owner asignado"
        )

    return get_siigo_free_trial(owner_email).model_dump_json()
