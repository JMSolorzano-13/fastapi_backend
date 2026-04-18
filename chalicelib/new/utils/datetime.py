from datetime import UTC, date, datetime

from dateutil import tz
from dateutil.relativedelta import relativedelta

PrimitiveType = str | int | float | bool | date | datetime
mx_tz = tz.gettz("America/Mexico_City")
utc_tz = tz.gettz("UTC")


def utc_now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def mx_now() -> datetime:
    return datetime.now(mx_tz).replace(tzinfo=None)


def mx_today() -> datetime:
    return mx_now().replace(hour=0, minute=0, second=0, microsecond=0)


def utc_to_mx(dt: datetime) -> datetime:
    return dt.replace(tzinfo=utc_tz).astimezone(mx_tz).replace(tzinfo=None)


def mx_to_utc(dt: datetime) -> datetime:
    return dt.replace(tzinfo=mx_tz).astimezone(utc_tz).replace(tzinfo=None)


def today_mx_in_utc() -> datetime:
    return mx_to_utc(mx_today())


def datetime_to_ASN_1(value: datetime) -> str:
    return value.strftime("%y%m%d%H%M%SZ")


def get_start_date_relativedelta(start: datetime, delta: relativedelta) -> datetime:
    first_of_current_month = start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return first_of_current_month + delta
