import random
from datetime import datetime, timedelta

from chalicelib.new.config.infra import envars
from chalicelib.new.query.domain.query import Query


def set_execute_at(query: Query):
    random_delay = random.randint(0, int(envars.sqs.PROCESS_PACKAGE_XML_MAX_DELAY.total_seconds()))
    execute_at = datetime.now() + timedelta(seconds=random_delay)
    query.execute_at = execute_at
