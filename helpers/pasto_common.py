"""Pasto webhook helper — extracted from chalicelib/blueprints/pasto/common.py.

Replaces bp-dependent version with explicit json_body/headers parameters.
"""

import json

from chalicelib.logger import DEBUG, ERROR, log
from chalicelib.modules import Modules


def parse_pasto_webhook(json_body: dict, headers: dict, webhook_name: str) -> tuple:
    debug_data = {
        "name": webhook_name,
        "body": json_body,
        "headers": headers,
    }
    log(
        Modules.ADD_WEBHOOK,
        DEBUG,
        "RECEIVED",
        debug_data,
    )
    error = json_body.get("Status", -1)
    if error != 0:
        log(
            Modules.ADD_WEBHOOK,
            ERROR,
            "FAILED",
            debug_data,
        )
        return error, None, headers
    pasto_body = json.loads(json_body["Body"])
    return error, pasto_body, headers
