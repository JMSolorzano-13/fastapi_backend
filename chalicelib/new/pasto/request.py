import enum
import json
from dataclasses import dataclass
from datetime import timedelta

from retrying import retry

from chalicelib.boto3_clients import s3_client
from chalicelib.logger import DEBUG, log
from chalicelib.modules import Modules
from chalicelib.new.config.infra import envars
from chalicelib.new.pasto.consumer import consume
from chalicelib.new.pasto.exception import PastoInternalError, PastoTimeoutError
from chalicelib.new.shared.domain.primitives import Identifier


@dataclass
class PastoRequest:
    ocp_key: str
    url: str

    def headers(self):
        return {
            "Ocp-Apim-Subscription-Key": self.ocp_key,
            "Content-Type": "application/json",
        }


@dataclass
class PastoRequestAuth(PastoRequest):
    authorization: str

    def headers(self):
        headers = super().headers()
        headers["Authorization"] = self.authorization
        return headers


class PastoHTTPMethods(enum.IntEnum):
    GET = 0
    POST = 1
    PUT = 2
    DELETE = 3
    HEAD = 4
    OPTIONS = 5
    PATCH = 6
    MERGE = 7
    COPY = 8


@dataclass
class PastoFileSenderResult:
    action: str
    quantity: int
    amount: float


@dataclass
class PastoFileSender(PastoRequestAuth):  # TODO unify more
    api_route: str
    endpoint: str

    bucket: str
    expires_in: timedelta

    resource_type = 0  # No Idea what this is

    def _upload_file(self, zip_content: bytes, path: str) -> str:
        s3_client().put_object(
            Body=zip_content,
            Bucket=self.bucket,
            Key=path,
        )
        url: str | None = s3_client().generate_presigned_url(
            "get_object",
            Params={
                "Bucket": self.bucket,
                "Key": path,
            },
            ExpiresIn=int(self.expires_in.total_seconds()),
        )
        assert url
        return url

    @retry(
        stop_max_attempt_number=envars.PASTO_MAX_RETRIES, wait_fixed=envars.PASTO_REQUEST_TIMEOUT
    )
    def _send_to_pasto(
        self,
        request_identifier: Identifier,
        company_identifier: Identifier,
        pasto_company_identifier: Identifier,
        s3_url: str,
    ) -> str:
        data = json.dumps(
            {
                "action_code": self.action_code,
                "parameters": {
                    "Receiver": {
                        "Endpoint": self.endpoint,
                        "ApiRoute": self.api_route,
                        "Method": self.method.value,
                        "Headers": [
                            {
                                "Key": "company_identifier",
                                "Value": company_identifier,
                            },
                            {
                                "Key": "request_identifier",
                                "Value": request_identifier,
                            },
                        ],
                    },
                    "metadataconfiguration": {
                        "CompanyId": pasto_company_identifier,
                    },
                    "resourceconfig": {
                        "uri": s3_url,
                        "resourcetype": self.resource_type,
                    },
                },
            }
        )
        headers = self.headers()
        url = f"{self.url}/{self.request_api_route}"

        context = {
            "action_code": self.action_code,
            "company_identifier": company_identifier,
            "request_identifier": request_identifier,
        }

        try:
            response = consume(
                url=url,
                headers=headers,
                data=data,
                timeout=envars.PASTO_REQUEST_TIMEOUT,
                debug_info={
                    "action_code": self.action_code,
                    "company_identifier": company_identifier,
                    "request_identifier": request_identifier,
                },
            )
            if response.status_code != 200:
                raise PastoInternalError(
                    url=url,
                    debug_info={
                        "action_code": self.action_code,
                        "company_identifier": company_identifier,
                        "request_identifier": request_identifier,
                    },
                )

        except PastoTimeoutError as e:
            log(
                Modules.ADD_FULL,
                DEBUG,
                "TIMEOUT",
                context | {"exception": e},
            )
            return None

        except PastoInternalError as e:
            log(
                Modules.ADD_FULL,
                DEBUG,
                "PASTO_ERROR",
                context | {"exception": e},
            )
            return None

        log(
            Modules.ADD_FULL,
            DEBUG,
            "ACTION_REQUESTED",
            context | {"response": response.json()},
        )

        return response.json().get("data")
