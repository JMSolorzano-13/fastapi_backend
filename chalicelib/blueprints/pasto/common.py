import json

from chalicelib.logger import DEBUG, ERROR, log
from chalicelib.modules import Modules


def bp_to_pasto_data(bp, webhook_name: str) -> tuple[int, None, None]:
    json_body = bp.current_request.json_body
    headers = bp.current_request.headers
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
