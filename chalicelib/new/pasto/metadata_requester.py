import json
from dataclasses import dataclass
from datetime import timedelta

from sqlalchemy.orm import Session

from chalicelib.boto3_clients import s3_client
from chalicelib.new.pasto.consumer import Response, consume
from chalicelib.new.pasto.paths import MetadataPath
from chalicelib.new.pasto.request import PastoHTTPMethods, PastoRequestAuth
from chalicelib.new.shared.domain.primitives import Identifier
from chalicelib.schema.models import Company as CompanyORM
from chalicelib.schema.models import Workspace as WorkspaceORM


@dataclass
class MetadataRequester(PastoRequestAuth):
    endpoint: str
    api_route: str
    session: Session
    bucket: str
    expiration: timedelta
    method: PastoHTTPMethods = PastoHTTPMethods.POST
    action_code = "contpaqi-add-get-document-empty-xml-report"
    request_api_route = "syncWorkerActions/add"

    def _request_metadata(
        self,
        company_identifier: Identifier,
        pasto_company_identifier: Identifier,
        workspace_identifier: Identifier,
        token: str = None,
    ) -> Response:
        url = f"{self.url}/{self.request_api_route}"

        metadata = MetadataPath(company_identifier=company_identifier)

        s3_url: str | None = s3_client().generate_presigned_url(
            "put_object",
            Params={
                "Bucket": self.bucket,
                "Key": metadata.path,
            },
            ExpiresIn=int(self.expiration.total_seconds()),
        )
        assert s3_url
        dot_com = ".com/"
        s3_url_base, s3_url_file = s3_url.split(dot_com)
        s3_url_base = f"{s3_url_base}{dot_com}"
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
                            }
                        ],
                    },
                    "dbconfiguration": {
                        "databasename": f"document_{pasto_company_identifier}_content",
                        "extras": [f"document_{pasto_company_identifier}_metadata"],
                    },
                    "bucketconfig": {
                        "endpoint": s3_url_base,
                        "bucketname": s3_url_file,
                        "method": PastoHTTPMethods.PUT.value,
                    },
                    "connector_ids": [workspace_identifier],
                },
                "connector_ids": [workspace_identifier],
            },
        )
        headers = self.headers()

        if token:
            headers["Authorization"] = token

        response = consume(
            url=url,
            headers=headers,
            data=data,
            debug_info={
                "company_identifier": company_identifier,
                "pasto_company_identifier": pasto_company_identifier,
            },
        )
        return data, response

    def request_metadata(self, company_identifier: Identifier) -> tuple[str, str]:
        company: CompanyORM = (
            self.session.query(CompanyORM).filter(CompanyORM.identifier == company_identifier).one()
        )
        workspace: WorkspaceORM = company.workspace

        self.authorization = workspace.pasto_worker_token

        payload, response = self._request_metadata(
            company_identifier,
            company.pasto_company_identifier,
            workspace.identifier,
        )

        return payload, response.text

    def set_pasto_last_metadata_sync_null(self, company_identifier: Identifier):
        self.session.query(CompanyORM).filter(
            CompanyORM.identifier == company_identifier,
        ).update(
            {
                CompanyORM.pasto_last_metadata_sync: None,
            }
        )
