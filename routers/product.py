"""Product routes — Stripe product webhook and product listing.

Ported from: backend/chalicelib/blueprints/product.py
2 routes total.
"""

from fastapi import APIRouter, Depends, Header, Request
from sqlalchemy.orm import Session

from chalicelib.new.config.infra import envars
from chalicelib.new.product.domain import ProductSetter
from chalicelib.new.product.infra import ProductRepositorySA
from chalicelib.new.product.infra.product_stripe_constructor import (
    ProductStripeConstructor,
)
from dependencies import get_db_session, get_db_session_rw
from helpers.product_helpers import get_list_of_products

router = APIRouter(tags=["Product"])


@router.post("/set")
async def set_product(
    request: Request,
    session: Session = Depends(get_db_session_rw),
):
    payload = await request.body()
    signature = request.headers["stripe-signature"]

    product_repo = ProductRepositorySA(session)
    product_setter = ProductSetter(product_repo)
    product_constructor = ProductStripeConstructor(envars.STRIPE_SET_PRODUCT_SECRET_KEY)
    product = product_constructor.construct(payload, signature)
    product_setter.set_product(product)


@router.get("/get_all")
def get_all(
    session: Session = Depends(get_db_session),
    access_token: str = Header(alias="access_token"),
):
    if not access_token:
        return 401

    return get_list_of_products(0)
