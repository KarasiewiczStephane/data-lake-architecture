"""MinIO/S3-compatible storage client using boto3.

Provides a unified interface for object storage operations that works
with both MinIO (local development) and AWS S3 (production).
"""

import io
import logging
from typing import BinaryIO, Iterator

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class MinIOClient:
    """S3-compatible client for MinIO and AWS S3.

    Wraps boto3 to provide a simplified interface for common object
    storage operations with automatic error handling.

    Args:
        endpoint: MinIO/S3 endpoint (e.g., 'localhost:9000').
        access_key: Access key for authentication.
        secret_key: Secret key for authentication.
        secure: Whether to use HTTPS. Defaults to False for local MinIO.
    """

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        secure: bool = False,
    ) -> None:
        self.endpoint = f"{'https' if secure else 'http'}://{endpoint}"
        self.s3 = boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version="s3v4"),
            region_name="us-east-1",
        )
        logger.info("Initialized storage client for %s", self.endpoint)

    def create_bucket(self, bucket_name: str) -> bool:
        """Create a bucket if it does not already exist.

        Args:
            bucket_name: Name of the bucket to create.

        Returns:
            True if bucket was created, False if it already exists.
        """
        try:
            self.s3.head_bucket(Bucket=bucket_name)
            logger.debug("Bucket %s already exists", bucket_name)
            return False
        except ClientError:
            self.s3.create_bucket(Bucket=bucket_name)
            logger.info("Created bucket %s", bucket_name)
            return True

    def upload_file(
        self,
        bucket: str,
        key: str,
        data: bytes | BinaryIO,
        metadata: dict[str, str] | None = None,
    ) -> str:
        """Upload a file to a bucket.

        Args:
            bucket: Target bucket name.
            key: Object key (path within the bucket).
            data: File content as bytes or file-like object.
            metadata: Optional key-value metadata to attach.

        Returns:
            S3 URI string (s3://bucket/key).
        """
        extra_args: dict = {}
        if metadata:
            extra_args["Metadata"] = {k: str(v) for k, v in metadata.items()}

        if isinstance(data, bytes):
            data = io.BytesIO(data)

        self.s3.upload_fileobj(data, bucket, key, ExtraArgs=extra_args or None)
        logger.debug("Uploaded %s/%s", bucket, key)
        return f"s3://{bucket}/{key}"

    def download_file(self, bucket: str, key: str) -> bytes:
        """Download file content from a bucket.

        Args:
            bucket: Source bucket name.
            key: Object key to download.

        Returns:
            File content as bytes.

        Raises:
            ClientError: If the object does not exist.
        """
        response = self.s3.get_object(Bucket=bucket, Key=key)
        return response["Body"].read()

    def list_objects(
        self, bucket: str, prefix: str = ""
    ) -> Iterator[dict[str, object]]:
        """List objects in a bucket with optional prefix filtering.

        Args:
            bucket: Bucket name to list.
            prefix: Key prefix for filtering results.

        Yields:
            Dicts with 'key', 'size', and 'modified' fields.
        """
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                yield {
                    "key": obj["Key"],
                    "size": obj["Size"],
                    "modified": obj["LastModified"],
                }

    def get_object_metadata(self, bucket: str, key: str) -> dict[str, str]:
        """Get user-defined metadata for an object.

        Args:
            bucket: Bucket name.
            key: Object key.

        Returns:
            Dictionary of user-defined metadata.
        """
        response = self.s3.head_object(Bucket=bucket, Key=key)
        return response.get("Metadata", {})

    def delete_object(self, bucket: str, key: str) -> None:
        """Delete an object from a bucket.

        Args:
            bucket: Bucket name.
            key: Object key to delete.
        """
        self.s3.delete_object(Bucket=bucket, Key=key)
        logger.debug("Deleted %s/%s", bucket, key)

    def object_exists(self, bucket: str, key: str) -> bool:
        """Check if an object exists in a bucket.

        Args:
            bucket: Bucket name.
            key: Object key to check.

        Returns:
            True if the object exists, False otherwise.
        """
        try:
            self.s3.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError:
            return False
