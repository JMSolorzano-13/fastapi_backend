from app import _scrap_cron


def test_scrap_cron_sqlalchemy():
    _scrap_cron(event={"offset": 0, "limit": 10}, context={})
