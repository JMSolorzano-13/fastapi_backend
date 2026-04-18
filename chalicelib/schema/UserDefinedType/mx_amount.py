from sqlalchemy import Numeric


class MXAmount(Numeric):
    """Identical to Numeric, only used to identify MXN amounts"""
