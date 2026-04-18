import inspect
import json
import logging.config
import logging.handlers
from datetime import datetime
from logging import CRITICAL, DEBUG, ERROR, INFO, WARNING  # noqa

from chalicelib.modules import Modules
from chalicelib.new.config.infra import envars
from chalicelib.new.utils.datetime import utc_now, utc_to_mx

EXCEPTION = CRITICAL

DEFAULT_LEVEL = envars.LOG_LEVEL


# LOGS PARA STRIPE
logging.getLogger("stripe").setLevel(logging.WARNING)


def setup_logging():
    logging.getLogger().setLevel(DEFAULT_LEVEL)
    logging.basicConfig(
        level=DEFAULT_LEVEL,
        format="%(message)s",
    )


def log(
    module: Modules,
    level: int,
    code: str,
    context: dict | None = None,
):
    try:
        module_name = str(module.name if isinstance(module, Modules) else module)
        logger = logging.getLogger(module_name)
        levels = {
            logging.DEBUG: "DEBUG",
            logging.INFO: "INFO",
            logging.WARNING: "WARNING",
            logging.ERROR: "ERROR",
            logging.CRITICAL: "CRITICAL",
        }
        context = {
            "timestamp": datetime.now().isoformat(),
            "timestamp-mx": utc_to_mx(utc_now()).isoformat(),
            "level": levels[level],
            "module": module_name,
            "log_code": code,
            "context": context,
        }
        message = json.dumps(
            context,
            default=str,
        )
        level = level
        exc_info = level >= EXCEPTION
        logger.log(
            level,
            message,
            extra={"log_code": code},
            exc_info=exc_info,
            stack_info=exc_info,
            stacklevel=2,
        )
    except Exception as e:
        logger.log(
            EXCEPTION,
            f"{message} {e}",
            extra={"log_code": code},
            exc_info=True,
            stack_info=True,
        )


LOG_IN_LIMIT = 100


def log_in(values):
    if len(values) < LOG_IN_LIMIT:
        return
    frame = inspect.currentframe().f_back
    filename = frame.f_code.co_filename
    lineno = frame.f_lineno
    place = f"{filename}:{lineno}"
    log(
        Modules.IN_OPERATOR,
        WARNING,
        "LOG_IN_LIMIT",
        {
            "place": place,
            "values": len(values),
        },
    )


setup_logging()
