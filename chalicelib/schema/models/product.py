from sqlalchemy import JSON, Column, Integer, String

from .model import BasicModel


class Product(BasicModel):
    __tablename__ = "product"

    stripe_identifier = Column(
        String,
        index=True,
        nullable=False,
        primary_key=True,
    )
    characteristics = Column(
        JSON,
        nullable=False,
    )
    price = Column(
        Integer,
        nullable=False,
    )
    stripe_price_identifier = Column(
        String,
        index=True,
        nullable=False,
    )
    stripe_name = Column(
        String,
        nullable=False,
        index=True,
    )
