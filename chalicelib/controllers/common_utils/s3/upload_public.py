import io
from datetime import timedelta

from boto3_type_annotations.s3 import Client as S3Client
from pydantic import HttpUrl


def _upload(s3_client: S3Client, data: bytes, bucket: str, key: str) -> None:
    s3_client.upload_fileobj(io.BytesIO(data), bucket, key)


def _get_public_url(
    s3_client: S3Client, bucket: str, key: str, expiration_delta: timedelta
) -> HttpUrl:
    url = s3_client.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=int(expiration_delta.total_seconds()),
    )
    assert url
    return HttpUrl(url)


def upload_public(
    s3_client: S3Client,
    data: bytes,
    bucket: str,
    key: str,
    expiration_delta: timedelta,
) -> HttpUrl:
    _upload(
        s3_client=s3_client,
        data=data,
        bucket=bucket,
        key=key,
    )
    url = _get_public_url(
        s3_client=s3_client,
        bucket=bucket,
        key=key,
        expiration_delta=expiration_delta,
    )
    return url
