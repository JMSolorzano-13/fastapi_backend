import base64
import hashlib
import logging
import re
import textwrap
from datetime import datetime

import requests

logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger(__name__)
REQUEST_TIMEOUT = 15

LOG = False


class RequestException(Exception):
    """If there is a problem in the request"""


def clean_xml(xml: str) -> str:
    """Clean a XML string to be used in SAT request.

    Removes all the spaces and new line characters between tags.

    Args:
        xml (str): XML to be cleaned.

    Returns:
        str: XML clean.
    """
    return xml.strip()


def remove_namespaces(xml):
    return re.sub(r"[souh]:", "", xml)


def prepare_template(template: str, data: dict) -> str:
    """Takes a XML template and fill the `variable` (data betwen {}) fields.

    Args:
        template (str): Template to be processed.
        data (dict): Variables to be replaced.

    Returns:
        str: Template with variables replaced.
    """
    template_clean = clean_xml(template)
    return template_clean.format(**data)


def binary_to_utf8(binary: bytes) -> str:
    """Takes a bytes object an returns the string represents it.

    Args:
        binary (bytes): Raw binary to be process.

    Returns:
        str: binary in base64 in utf-8.
    """
    return base64.encodebytes(binary).decode("UTF-8")


def digest(data: str) -> str:
    return binary_to_utf8(hashlib.sha1(data.encode("UTF-8")).digest())[:-1]


def der_to_pem(der_data: str, cert_type: str) -> str:
    """Convert DER data into PEM.

    Args:
        der_data (str): DER data to be convert.
        cert_type (str): Type of certificate to be created
                         (`ENCRYPTED PRIVATE KEY`, `CERTIFICATE`, etc).

    Returns:
        str: Certificate converted.
    """
    wrapped = "\n".join(textwrap.wrap(der_data, 64))
    return f"-----BEGIN {cert_type}-----\n{wrapped}\n-----END {cert_type}-----\n"


def consume(soap_action, uri, body, token=None) -> requests.Response:
    name = f"logs/{uri.split('/')[-1]}-{datetime.now().isoformat()}"
    if LOG:
        with open(f"{name}-content.xml", "w") as f:
            f.write(body)
    headers = {
        "Content-type": 'text/xml; charset="utf-8"',
        "Accept": "text/xml",
        "Cache-Control": "no-cache",
        "SOAPAction": soap_action,
    }
    if token:
        headers["Authorization"] = f'WRAP access_token="{token}"'
    result = requests.post(uri, body, headers=headers, timeout=REQUEST_TIMEOUT)
    if LOG:
        with open(f"{name}-response.xml", "w") as f:
            f.write(result.text)
    return result


def check_response(response: requests.Response):
    if response.status_code != 200:
        raise RequestException(response.status_code, response.reason)


def handle_special_characters_in_rfc(rfc: str) -> str:
    """Check if the RFC contains letter Ñ.

    Args:
        rfc (str): RFC to be checked.

    Returns:
        str: RFC corrected.
    """

    if "Ñ" in rfc:
        return rfc.replace("Ñ", "&#209;")
    return rfc
