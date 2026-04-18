import io
import os
from collections.abc import Iterable
from dataclasses import dataclass

from botocore.exceptions import ClientError

from chalicelib.new.config.infra import envars
from chalicelib.new.package.domain.package import Package
from chalicelib.new.shared.domain.primitives import Identifier
from chalicelib.new.shared.infra.s3_repository import S3Repository


@dataclass
class PackageRepositoryS3(S3Repository):
    def copy_mock(self, package_id: str):
        self.s3_client.copy(
            CopySource={
                "Bucket": self.bucket_url,
                "Key": self._get_mock_route(package_id),
            },
            Bucket=self.bucket_url,
            Key=self._get_route(package_id),
        )

    def save(self, package: Package):
        if envars.LOCAL_INFRA:
            return self._local_save(package)
        return self._remote_save(package)

    def _local_save(self, package: Package):
        path = self._get_route(package.sat_uuid)
        with open(path, "wb") as file:
            file.write(package.zip_content)

    def _remote_save(self, package: Package):
        path = self._get_route(package.sat_uuid)
        self.s3_client.upload_fileobj(
            Fileobj=io.BytesIO(package.zip_content),
            Bucket=self.bucket_url,
            Key=path,
        )

    def get_from_sat_uuid(self, sat_uuid: Identifier) -> Package:
        if envars.LOCAL_INFRA:
            return self._local_get_from_sat_uuid(sat_uuid)
        return self._remote_get_from_sat_uuid(sat_uuid)

    def _local_get_from_sat_uuid(self, sat_uuid: Identifier) -> Package:
        with open(self._get_route(sat_uuid), "rb") as file:
            return Package(sat_uuid=sat_uuid, zip_content=file.read())

    def _remote_get_from_sat_uuid(self, sat_uuid: Identifier) -> Package:
        path = self._get_route(sat_uuid)
        content = io.BytesIO()
        try:
            self.s3_client.download_fileobj(Bucket=self.bucket_url, Key=path, Fileobj=content)
        except ClientError as e:
            raise ValueError(f"Package with path {path} not found, bucket {self.bucket_url}") from e
        return Package(sat_uuid=sat_uuid, zip_content=content.getvalue())

    def get_from_sat_uuids(self, sat_uuids: Iterable[str]) -> Iterable[Package]:
        return (self.get_from_sat_uuid(sat_uuid) for sat_uuid in sat_uuids)  # TODO async

    def _get_route(self, package_id: str) -> str:
        if envars.LOCAL_INFRA and not os.path.exists("Zips"):
            os.makedirs("Zips")
        return f"Zips/{package_id}.zip"

    def _get_mock_route(self, package_id: str) -> str:
        return f"mock/{package_id}.zip"
