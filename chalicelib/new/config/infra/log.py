import logging

logger = logging.getLogger(__name__)
CFDI_TO_FIX = logging.ERROR // 2 + logging.WARNING // 2


def set_global_logger(local_logger: logging.Logger):
    global logger  # pylint: disable=global-statement
    logger = local_logger
