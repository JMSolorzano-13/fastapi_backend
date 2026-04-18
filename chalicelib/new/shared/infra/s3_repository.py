from abc import ABC
from dataclasses import dataclass

from boto3_type_annotations.s3 import Client as S3Client


@dataclass
class S3Repository(ABC):
    bucket_url: str
    s3_client: S3Client
