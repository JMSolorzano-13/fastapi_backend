from datetime import datetime

import freezegun
from dateutil.relativedelta import relativedelta

from chalicelib.new.utils.datetime import get_start_date_relativedelta

NOW_FREEZED = datetime.fromisoformat("2024-06-15T10:30:00")


@freezegun.freeze_time(NOW_FREEZED)
def test_get_current_and_prev_month_range():
    months = 1
    prev_month_start = get_start_date_relativedelta(NOW_FREEZED, relativedelta(months=-months))
    assert prev_month_start.day == 1
    assert prev_month_start.second == 0
    assert prev_month_start.minute == 0
    assert prev_month_start.hour == 0
    diff = NOW_FREEZED - prev_month_start
    assert diff.days >= 28  # Min days in month
    assert diff.days <= 62  # Max days in two months
    nex_month = get_start_date_relativedelta(prev_month_start, relativedelta(months=months))
    assert nex_month.month == NOW_FREEZED.month
    assert nex_month.year == NOW_FREEZED.year
