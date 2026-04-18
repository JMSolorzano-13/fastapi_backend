from typing import Any

import requests
from requests import Response  # noqa

from chalicelib.new.config.infra import envars
from chalicelib.new.pasto.exception import PastoInternalError, PastoTimeoutError


def consume(
    url: str,
    headers: dict | None = None,
    data: Any = None,
    timeout: int = envars.GENERIC_TIMEOUT,
    debug_info: dict | None = None,
) -> Response:
    """
    :raises PastoTimeoutError: if the request fails due to a timeout
    :raises PastoInternalError: if the request fails due to an internal error
    """
    debug_info = debug_info or {}
    try:
        response = requests.post(url, headers=headers, data=data, timeout=timeout)
        if response.status_code != 200:
            debug_info["response"] = response.__dict__
            raise PastoInternalError(url=url, debug_info=debug_info)
    except requests.exceptions.ReadTimeout as e:
        raise PastoTimeoutError(url=url, debug_info=debug_info) from e
    return response
