import io
from dataclasses import dataclass, field

from botocore.exceptions import ClientError
from pycfdi_credentials import Certificate, PrivateKey
from sqlalchemy.orm import Session

from chalicelib.logger import DEBUG, log
from chalicelib.modules import Modules
from chalicelib.new.fiel.domain import FIEL
from chalicelib.new.fiel.domain.exceptions import CertsNotFound
from chalicelib.new.fiel.domain.file_type import FileType
from chalicelib.new.shared.domain.primitives import Identifier
from chalicelib.new.shared.infra.s3_repository import S3Repository
from chalicelib.schema.models import (
    Company as CompanyORM,  # TODO remove when company data don't depend on the DB
)


@dataclass
class FielRepositoryS3(S3Repository):
    session: Session  # TODO remove when company data don't depend on the DB
    bucket_url: str
    wid_cid: dict[str, tuple[int, int]] = field(default_factory=dict)

    def get_from_company_identifier(self, company_identifier: Identifier) -> FIEL:
        try:
            cer, key, txt = self._get_files(company_identifier)
        except CertsNotFound:
            self.mark_as_not_valid_certs(company_identifier)
            raise
        return FIEL(
            certificate=Certificate(
                certificate=cer,
            ),
            private_key=PrivateKey(
                content=key,
                passphrase=txt,
            ),
        )

    def mark_as_not_valid_certs(self, company_identifier: Identifier):
        company = (
            self.session.query(CompanyORM).filter(CompanyORM.identifier == company_identifier).one()
        )
        log(
            Modules.FIEL,
            DEBUG,
            "NO_LONGER_VALID_CERTS",
            {
                "company_identifier": company_identifier,
            },
        )
        company.has_valid_certs = False

    def _get_files(self, company_identifier: Identifier) -> tuple[bytes, ...]:
        cer_file = io.BytesIO()
        key_file = io.BytesIO()
        txt_file = io.BytesIO()
        routes = {
            FileType.CERTIFICATE: self._get_route(company_identifier, FileType.CERTIFICATE),
            FileType.PRIVATE_KEY: self._get_route(company_identifier, FileType.PRIVATE_KEY),
            FileType.PASSPHRASE: self._get_route(company_identifier, FileType.PASSPHRASE),
        }
        try:
            self.s3_client.download_fileobj(
                Bucket=self.bucket_url,
                Key=routes[FileType.CERTIFICATE],
                Fileobj=cer_file,
            )
            self.s3_client.download_fileobj(
                Bucket=self.bucket_url,
                Key=routes[FileType.PRIVATE_KEY],
                Fileobj=key_file,
            )
            self.s3_client.download_fileobj(
                Bucket=self.bucket_url,
                Key=routes[FileType.PASSPHRASE],
                Fileobj=txt_file,
            )
        except ClientError as e:
            raise CertsNotFound(company_identifier) from e
        cer_file.seek(0)
        key_file.seek(0)
        txt_file.seek(0)
        return (
            cer_file.read(),
            key_file.read(),
            txt_file.read(),
        )

    def get_company_ws_id_and_id(self, company_identifier: Identifier) -> tuple[int, int]:
        if company_identifier not in self.wid_cid:
            company_orm = (
                self.session.query(CompanyORM.workspace_id, CompanyORM.id)
                .filter(CompanyORM.identifier == company_identifier)
                .one()
            )
            self.wid_cid[company_identifier] = company_orm.workspace_id, company_orm.id
        return self.wid_cid[company_identifier]

    def _get_route(self, company_identifier: Identifier, file_type: FileType) -> str:
        # TODO remove when company data don't depend on the DB
        wid, cid = self.get_company_ws_id_and_id(company_identifier)
        ext = file_type.value.split(".")[-1]
        return f"ws_{wid}/c_{cid}.{ext}"
