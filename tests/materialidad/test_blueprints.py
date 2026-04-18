import urllib.parse
from collections.abc import Callable
from http import HTTPStatus

import pytest
from chalice.test import Client
from flask import json
from sqlalchemy.orm import Session

from chalicelib.blueprints.attachment import CreateRequest
from chalicelib.controllers.cfdi import CFDIController
from chalicelib.schema.models.company import Company
from chalicelib.schema.models.tenant.cfdi import CFDI

# @bp.route("/{cid}/{uuid}", methods=["POST"], cors=common.cors_config, read_only=False)
# @bp.route("/{cid}/{uuid}", methods=["GET"], cors=common.cors_config)
# @bp.route("/{cid}/{uuid}/{file_name}", methods=["DELETE"], cors=common.cors_config, read_only=False)
BASE_PATH = "/Attachment"


def test_create(
    client_authenticated: Client,
    company: Company,
    cfdi: CFDI,
    attachments_data_factory: Callable[[int], CreateRequest],
):
    attachments_data: CreateRequest = attachments_data_factory(2)
    result = client_authenticated.http.post(
        f"{BASE_PATH}/{company.identifier}/{cfdi.UUID}",
        body=attachments_data.model_dump_json(),
    )
    assert result.status_code == HTTPStatus.OK, result.json_body
    assert result.json_body.keys() == {item.file_name for item in attachments_data.items}
    assert all(url.startswith("https://") for url in result.json_body.values())


def test_download(
    client_authenticated: Client,
    company: Company,
    cfdi: CFDI,
    attachments: dict[str, str],
):
    result = client_authenticated.http.get(f"{BASE_PATH}/{company.identifier}/{cfdi.UUID}")
    assert result.status_code == HTTPStatus.OK, result.json_body
    assert result.json_body.keys() == attachments.keys()
    assert all(url.startswith("https://") for url in result.json_body.values())


def test_delete(
    client_authenticated: Client,
    company: Company,
    cfdi: CFDI,
    attachments: dict[str, str],
):
    file_name_to_delete = next(iter(attachments.keys()))
    result = client_authenticated.http.delete(
        f"{BASE_PATH}/{company.identifier}/{cfdi.UUID}/{file_name_to_delete}"
    )
    assert result.status_code == HTTPStatus.OK, result.json_body
    assert result.json_body == {
        "message": f"Attachment {file_name_to_delete} deleted successfully from CFDI {cfdi.UUID}"
    }


def test_search_by_attachment_file_name(
    client_authenticated: Client,
    company: Company,
    cfdi: CFDI,
    attachments: dict[str, str],
):
    # Search for an existing attachment
    file_name_to_search = next(iter(attachments.keys()))
    search_payload = {
        "domain": [
            ("company_identifier", "=", str(company.identifier)),
            ("attachments_count", ">", 0),
            ("attachments", "=", "any"),
            ("attachments.file_name", "like", file_name_to_search[5:10]),
        ],
        "fields": [
            "UUID",
            "attachments.file_name",
        ],
    }
    result = client_authenticated.http.post(
        "/CFDI/search",
        body=json.dumps(search_payload),
    )
    assert result.status_code == HTTPStatus.OK, result.json_body
    assert result.json_body["total_records"] == 1
    assert any(
        a["file_name"] == file_name_to_search for a in result.json_body["data"][0]["attachments"]
    )


def test_search_like_by_attachment_file_name(
    attachments: dict[str, str],
    company_session: Session,
):
    # Search for an existing attachment
    file_name_to_search = next(iter(attachments.keys()))
    res = CFDIController._search(
        domain=[
            ("attachments.file_name", "like", file_name_to_search[5:10]),
        ],
        fields=[
            "UUID",
            "attachments.file_name",
        ],
        session=company_session,
    )
    assert len(res) == 1  # type: ignore


@pytest.mark.parametrize(
    "file_name",
    [
        "file name with spaces.pdf",
        "file@name#with$special&chars!.pdf",
        "文件名含有非ASCII字符.pdf",
        "file-name-with-üñíçødé.pdf",
        "🐛.pdf",
    ],
)
def test_delete_name_with_non_url_safe(
    client_authenticated: Client,
    cfdi: CFDI,
    file_name: str,
    company: Company,
    attachments_data_factory: Callable[[int], CreateRequest],
):
    attachments_data = attachments_data_factory(1)
    attachments_data.items[0].file_name = file_name

    result = client_authenticated.http.post(
        f"{BASE_PATH}/{company.identifier}/{cfdi.UUID}",
        body=attachments_data.model_dump_json(),
    )
    assert result.status_code == HTTPStatus.OK, result.json_body

    url_downloads = client_authenticated.http.get(f"{BASE_PATH}/{company.identifier}/{cfdi.UUID}")
    assert url_downloads.status_code == HTTPStatus.OK, url_downloads.json_body
    assert file_name in url_downloads.json_body

    file_name_url_safe = urllib.parse.quote(file_name)
    delete_response = client_authenticated.http.delete(
        f"{BASE_PATH}/{company.identifier}/{cfdi.UUID}/{file_name_url_safe}"
    )
    assert delete_response.status_code == HTTPStatus.OK, delete_response.json_body
    assert delete_response.json_body == {
        "message": f"Attachment {file_name} deleted successfully from CFDI {cfdi.UUID}"
    }
