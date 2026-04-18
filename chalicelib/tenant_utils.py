from collections.abc import Mapping
from typing import Any


def check_body(json_body: dict[str, Any]) -> str | None:
    return json_body.get("company_identifier")


def check_domain(json_body: dict[str, Any]) -> str | None:
    domain = json_body.get("domain", [])
    # domains is a list of lists
    # Get the first element of the domain list in which the first element is `company_identifier`
    for ix, t in enumerate(domain):
        if not t or not isinstance(t, list):
            continue
        if t[0] == "company_identifier" and t[1] == "=":
            domain.pop(ix)
            return t[2]
    return None


def check_uri_params(uri_params: dict[str, Any]) -> str | None:
    return uri_params.get("company_identifier", uri_params.get("cid"))


def check_header(headers: Mapping) -> str | None:
    return headers.get("company_identifier")
